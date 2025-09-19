from __future__ import annotations

import csv
import io
import json
import os
import re
import time
from datetime import date, datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from flask import Blueprint, jsonify, request
from sqlalchemy import create_engine, inspect, text

from core.datasources import DatasourceRegistry
from core.settings import Settings
from core.sql_exec import get_mem_engine

from apps.dw.clarifier import propose_clarifying_questions
from apps.dw.llm import _unexpected_binds, nl_to_sql_with_llm
from apps.dw.patterns import parse_timeframe

if TYPE_CHECKING:  # pragma: no cover
    from core.pipeline import Pipeline


NAMESPACE = "dw::common"
dw_bp = Blueprint("dw", __name__)


@dw_bp.route("/model/info", methods=["GET"])
def model_info_route():
    from core.model_loader import model_info as _model_info

    return jsonify(_model_info())


def create_dw_blueprint(settings: Settings | None = None, pipeline: "Pipeline | None" = None) -> Blueprint:
    """Return the DocuWare blueprint wired to the provided pipeline/settings."""
    if settings is not None:
        try:
            settings.set_namespace(NAMESPACE)
        except AttributeError:
            pass
    return dw_bp


DATE_WINDOW_PAT = re.compile(
    r"(?i)\b(last|next|past|coming)\s+\d+\s+(day|days|week|weeks|month|months|year|years)|"
    r"between\s+\d{4}-\d{2}-\d{2}\s+(?:and|to)\s+\d{4}-\d{2}-\d{2}|"
    r"from\s+\d{4}-\d{2}-\d{2}\s+(?:and|to)\s+\d{4}-\d{2}-\d{2}"
)


def _wants_time_window(question: str) -> bool:
    return bool(DATE_WINDOW_PAT.search(question or ""))


def _compute_window(question: str, now: datetime) -> Dict[str, datetime]:
    reference = now if now.tzinfo is not None else now.replace(tzinfo=timezone.utc)
    start, end, _, _ = parse_timeframe(question, now=reference)
    if start and end:
        return {"date_start": start, "date_end": end}

    direct = re.search(
        r"(?i)(?:between|from)\s+(\d{4}-\d{2}-\d{2})\s+(?:and|to)\s+(\d{4}-\d{2}-\d{2})",
        question or "",
    )
    if direct:
        try:
            start_dt = datetime.fromisoformat(direct.group(1))
            end_dt = datetime.fromisoformat(direct.group(2))
        except ValueError:
            return {}
        if end_dt < start_dt:
            start_dt, end_dt = end_dt, start_dt
        return {
            "date_start": start_dt,
            "date_end": end_dt + timedelta(days=1),
        }

    return {}


def _looks_like_select(sql: str) -> bool:
    snippet = sql.strip().lstrip("(").strip()
    return snippet.upper().startswith("SELECT") or snippet.upper().startswith("WITH")


def _has_dml(sql: str) -> bool:
    upper = sql.upper()
    return any(token in upper for token in (" INSERT ", " UPDATE ", " DELETE ", " MERGE ", " DROP ", " ALTER "))


def _binds_required_in_sql(sql: str) -> List[str]:
    return re.findall(r":([A-Za-z_]\w*)", sql)


def _find_named_binds(sql: str) -> set[str]:
    return set(re.findall(r":([a-zA-Z_][a-zA-Z0-9_]*)", sql or ""))


def _pg(conn_str: str):
    return create_engine(conn_str, pool_pre_ping=True, future=True)


def _load_prompt_snippets(mem_engine, limit: int = 3) -> List[Tuple[str, str]]:
    shots: List[Tuple[str, str]] = []
    if mem_engine is None:
        return shots
    with mem_engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT title, COALESCE(sql_template, sql_raw) AS sql_body
                  FROM mem_snippets
                 WHERE namespace = :ns
                   AND COALESCE(is_verified, false) = true
                   AND COALESCE(tags, '[]'::jsonb) @> CAST(:tag_dw AS jsonb)
                   AND COALESCE(tags, '[]'::jsonb) @> CAST(:tag_oracle AS jsonb)
                   AND COALESCE(tags, '[]'::jsonb) @> CAST(:tag_contracts AS jsonb)
              ORDER BY updated_at DESC NULLS LAST
                 LIMIT :limit
                """
            ),
            {
                "ns": NAMESPACE,
                "tag_dw": json.dumps(["dw"]),
                "tag_oracle": json.dumps(["oracle"]),
                "tag_contracts": json.dumps(["contracts"]),
                "limit": limit,
            },
        ).mappings()
        for row in rows:
            sql = row.get("sql_body")
            title = row.get("title") or "example"
            if sql:
                shots.append((title, sql))
    return shots


def _insert_inquiry(
    mem,
    namespace: str,
    question: str,
    email: Optional[str],
    prefixes: List[str],
    status: str,
) -> int:
    with mem.begin() as con:
        result = con.execute(
            text(
                """
                INSERT INTO mem_inquiries(namespace, question, auth_email, prefixes, status, created_at, updated_at)
                VALUES (:ns, :q, :email, CAST(:pfx AS jsonb), :status, NOW(), NOW())
                RETURNING id
                """
            ),
            {
                "ns": namespace,
                "q": question,
                "email": email,
                "pfx": json.dumps(prefixes),
                "status": status,
            },
        )
        return int(result.scalar_one())


def _insert_run(
    mem,
    namespace: str,
    user_id: str,
    input_query: str,
    sql: str,
    status: str,
    rows: int,
    sample: Optional[List[Dict[str, Any]]],
) -> int:
    with mem.begin() as con:
        result = con.execute(
            text(
                """
                INSERT INTO mem_runs(namespace, user_id, input_query, sql_generated, sql_final, status, rows_returned, result_sample, created_at)
                VALUES (:ns, :uid, :iq, :sg, :sf, :st, :rows, CAST(:sample AS jsonb), NOW())
                RETURNING id
                """
            ),
            {
                "ns": namespace,
                "uid": user_id,
                "iq": input_query,
                "sg": sql,
                "sf": sql,
                "st": status,
                "rows": rows,
                "sample": json.dumps(sample or []),
            },
        )
        return int(result.scalar_one())


def _csv_bytes(rows: List[Dict[str, Any]]) -> bytes:
    if not rows:
        return b""
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    for row in rows:
        writer.writerow({key: ("" if value is None else value) for key, value in row.items()})
    return buffer.getvalue().encode("utf-8")


def run_sql_oracle(engine, sql: Optional[str], binds: Dict[str, Any]):
    if not sql:
        return False, None, {}, "sql_missing"
    try:
        with engine.connect() as conn:
            result = conn.execute(text(sql), binds or {})
            columns = list(result.keys())
            fetched: List[Dict[str, Any]] = []
            for record in result.fetchall():
                fetched.append({col: record[idx] for idx, col in enumerate(columns)})
    except Exception as exc:
        return False, None, {}, str(exc)
    meta = {"columns": columns, "rowcount": len(fetched)}
    return True, fetched, meta, None


def rows_suspiciously_empty(rows: List[Dict[str, Any]], question: str) -> bool:
    if rows:
        return False
    lowered = question.lower()
    safe_keywords = ["count", "how many", "total", "sum", "average", "avg"]
    if any(keyword in lowered for keyword in safe_keywords):
        return False
    return True


def _coerce_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value)
            if isinstance(parsed, datetime):
                return parsed
        except Exception:
            try:
                parsed_date = datetime.strptime(value, "%Y-%m-%d")
                return parsed_date
            except Exception:
                return None
    return None


def save_learning_artifacts(
    namespace: str,
    question: str,
    sql: str,
    rows: List[Dict[str, Any]],
    *,
    intent: Optional[Dict[str, Any]] = None,
    tags: Optional[List[str]] = None,
    settings: Optional[Settings] = None,
    date_column: Optional[str] = None,
    window_label: Optional[str] = None,
):
    mem_settings = settings or Settings(namespace=namespace)
    mem = get_mem_engine(mem_settings)
    if mem is None:
        return None
    tag_payload = list(tags or ["dw", "contracts", "autosave"])
    if date_column and "window" not in tag_payload:
        tag_payload.append("window")
    if "autosave" not in tag_payload:
        tag_payload.append("autosave")
    with mem.begin() as conn:
        run_id = conn.execute(
            text(
                """
            INSERT INTO mem_runs(namespace, input_query, interpreted_intent, sql_generated, sql_final, status, rows_returned, result_sample)
            VALUES (:ns, :q, :intent, :sql, :sql, 'complete', :nrows, :sample)
            RETURNING id
            """
            ),
            {
                "ns": namespace,
                "q": question,
                "intent": json.dumps(intent) if intent else None,
                "sql": sql,
                "nrows": len(rows),
                "sample": json.dumps(rows[:10]),
            },
        ).scalar_one()

        filters_payload = None
        if date_column:
            filters_payload = [
                [date_column, ">=", ":date_start"],
                [date_column, "<", ":date_end"],
            ]
        snippet_title = f"Contracts NLQ: {question[:64]}"
        snippet_desc = f"Auto-saved from NLQ: {question}"
        if window_label:
            snippet_desc += f" ({window_label})"
        conn.execute(
            text(
                """
            INSERT INTO mem_snippets(namespace, title, description, sql_template, sql_raw, input_tables, parameters, tags, is_verified, usage_count)
            VALUES (:ns, :title, :desc, :tmpl, :raw, :tables, :params, :tags, TRUE, 1)
            """
            ),
            {
                "ns": namespace,
                "title": snippet_title,
                "desc": snippet_desc,
                "tmpl": sql,
                "raw": sql,
                "tables": json.dumps(["Contract"]),
                "params": json.dumps(
                    {
                        "time": intent.get("time") if intent else None,
                        "filters": filters_payload,
                        "date_column": date_column,
                    }
                ),
                "tags": json.dumps(tag_payload),
            },
    )
    return run_id


def save_success_snippet(conn, namespace: str, question: str, sql: str, tags: List[str]):
    tag_payload = sorted(set(tags) | {"dw", "auto_learn"})
    conn.execute(
        text(
            """
        INSERT INTO mem_snippets(namespace, title, description, sql_template, sql_raw, input_tables, filters_applied, tags, is_verified, created_at, updated_at)
        VALUES(:ns, :title, :desc, :tmpl, :raw, :tabs::jsonb, :filters::jsonb, :tags::jsonb, true, NOW(), NOW())
        """
        ),
        {
            "ns": namespace,
            "title": f"DW answer: {question[:160]}",
            "desc": "Auto-learned from successful run.",
            "tmpl": sql,
            "raw": sql,
            "tabs": '["Contract"]',
            "filters": "[]",
            "tags": json.dumps(tag_payload, ensure_ascii=False),
        },
    )


def _learn_snippet(conn, question: str, sql: str, tags):
    conn.execute(
        text(
            """
        INSERT INTO mem_snippets(namespace, title, description, sql_raw, input_tables, tags, is_verified, source)
        VALUES (:ns, :title, :desc, :sql, '["Contract"]'::jsonb, CAST(:tags AS jsonb), false, 'dw')
        """
        ),
        {
            "ns": NAMESPACE,
            "title": question[:200],
            "desc": "Auto-learned from successful DW answer",
            "sql": sql,
            "tags": json.dumps(tags, ensure_ascii=False),
        },
    )


def _learn_mappings(conn):
    pairs = [
        ("stakeholder", "CONTRACT_STAKEHOLDER_*", "term"),
        ("department", "OWNER_DEPARTMENT", "term"),
        ("gross value", "NVL(CONTRACT_VALUE_NET_OF_VAT,0)+NVL(VAT,0)", "metric"),
        ("end date", "END_DATE", "column"),
        ("request date", "REQUEST_DATE", "column"),
        ("owner", "CONTRACT_OWNER", "term"),
    ]
    for alias, canonical, mapping_type in pairs:
        conn.execute(
            text(
                """
            INSERT INTO mem_mappings(namespace, alias, canonical, mapping_type, scope, source, confidence)
            VALUES (:ns, :alias, :canonical, :mtype, 'global', 'auto', 0.90)
            ON CONFLICT (namespace, alias, mapping_type, scope)
            DO UPDATE SET canonical = EXCLUDED.canonical, updated_at = NOW()
            """
            ),
            {
                "ns": NAMESPACE,
                "alias": alias,
                "canonical": canonical,
                "mtype": mapping_type,
            },
        )


def _infer_value_type(value):
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int) and not isinstance(value, bool):
        return "int"
    if isinstance(value, float):
        return "float"
    if isinstance(value, (list, dict)):
        return "json"
    return "string"


def _manual_upsert_setting(
    conn,
    *,
    key: str,
    value,
    value_type: str | None = None,
    scope: str = "namespace",
    scope_id=None,
    updated_by: str = "dw",
    is_secret: bool = False,
):
    vtype = value_type or _infer_value_type(value)
    value_json = json.dumps(value, ensure_ascii=False)
    update_stmt = text(
        """
        UPDATE mem_settings
           SET value = CAST(:val AS jsonb),
               value_type = :vtype,
               updated_by = :upd_by,
               updated_at = NOW(),
               is_secret  = :secret
         WHERE namespace = :ns
           AND key       = :key
           AND scope     = :scope
           AND ((:scope_id IS NULL AND scope_id IS NULL) OR scope_id = :scope_id)
        """
    )
    result = conn.execute(
        update_stmt,
        {
            "ns": NAMESPACE,
            "key": key,
            "val": value_json,
            "vtype": vtype,
            "scope": scope,
            "scope_id": scope_id,
            "upd_by": updated_by,
            "secret": is_secret,
        },
    )
    if result.rowcount and result.rowcount > 0:
        return

    insert_stmt = text(
        """
        INSERT INTO mem_settings(namespace, key, value, value_type, scope, scope_id,
                                 overridable, updated_by, created_at, updated_at, is_secret)
        VALUES (:ns, :key, CAST(:val AS jsonb), :vtype, :scope, :scope_id,
                true, :upd_by, NOW(), NOW(), :secret)
        """
    )
    conn.execute(
        insert_stmt,
        {
            "ns": NAMESPACE,
            "key": key,
            "val": value_json,
            "vtype": vtype,
            "scope": scope,
            "scope_id": scope_id,
            "upd_by": updated_by,
            "secret": is_secret,
        },
    )


def _ensure_mem_snapshot_schema(mem_engine) -> None:
    with mem_engine.begin() as conn:
        conn.execute(
            text(
                """
                ALTER TABLE mem_snapshots
                ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ
                """
            )
        )
        conn.execute(
            text(
                """
                UPDATE mem_snapshots
                   SET updated_at = COALESCE(updated_at, created_at)
                 WHERE updated_at IS NULL
                """
            )
        )


def _seed_semantic_layer(mem_engine) -> Dict[str, List[str]]:
    """Seed baseline metrics and mappings required for DW answering."""

    required_tables = ["Contract"]
    required_columns = [
        "CONTRACT_VALUE_NET_OF_VAT",
        "VAT",
        "START_DATE",
        "END_DATE",
        "REQUEST_DATE",
        "CONTRACT_STAKEHOLDER_1",
        "DEPARTMENT_1",
        "CONTRACT_STAKEHOLDER_2",
        "DEPARTMENT_2",
        "CONTRACT_STAKEHOLDER_3",
        "DEPARTMENT_3",
        "CONTRACT_STAKEHOLDER_4",
        "DEPARTMENT_4",
        "CONTRACT_STAKEHOLDER_5",
        "DEPARTMENT_5",
        "CONTRACT_STAKEHOLDER_6",
        "DEPARTMENT_6",
        "CONTRACT_STAKEHOLDER_7",
        "DEPARTMENT_7",
        "CONTRACT_STAKEHOLDER_8",
        "DEPARTMENT_8",
    ]

    payload = {
        "ns": NAMESPACE,
        "key": "contract_value_gross",
        "name": "Contract Value (Gross)",
        "desc": "Gross value = net + VAT",
        "calc": "NVL(CONTRACT_VALUE_NET_OF_VAT,0) + NVL(VAT,0)",
        "rt": json.dumps(required_tables),
        "rc": json.dumps(required_columns),
    }
    metric_sql = text(
        """
        INSERT INTO mem_metrics(namespace, metric_key, metric_name, description,
                                calculation_sql, required_tables, required_columns,
                                category, owner, is_active)
        VALUES(:ns, :key, :name, :desc, :calc,
               CAST(:rt AS jsonb), CAST(:rc AS jsonb),
               'contracts','dw', true)
        ON CONFLICT (namespace, metric_key, version) DO UPDATE
          SET calculation_sql = EXCLUDED.calculation_sql,
              description      = EXCLUDED.description,
              updated_at       = NOW()
        """
    )

    seeded = {"metrics": [], "mappings": []}

    with mem_engine.begin() as conn:
        conn.execute(metric_sql, payload)
        seeded["metrics"].append(payload["key"])

        for slot in range(1, 9):
            conn.execute(
                text(
                    """
                    INSERT INTO mem_mappings(namespace, alias, canonical, mapping_type, scope, source, confidence)
                    VALUES (:ns, :alias, :canonical, 'column', 'global', 'dw_seed', 0.98)
                    ON CONFLICT (namespace, alias, mapping_type, scope) DO UPDATE
                      SET canonical = EXCLUDED.canonical,
                          confidence = EXCLUDED.confidence,
                          updated_at = NOW()
                    """
                ),
                {
                    "ns": NAMESPACE,
                    "alias": f"CONTRACT_STAKEHOLDER_{slot}",
                    "canonical": "stakeholder",
                },
            )
            conn.execute(
                text(
                    """
                    INSERT INTO mem_mappings(namespace, alias, canonical, mapping_type, scope, source, confidence)
                    VALUES (:ns, :alias, :canonical, 'column', 'global', 'dw_seed', 0.95)
                    ON CONFLICT (namespace, alias, mapping_type, scope) DO UPDATE
                      SET canonical = EXCLUDED.canonical,
                          confidence = EXCLUDED.confidence,
                          updated_at = NOW()
                    """
                ),
                {
                    "ns": NAMESPACE,
                    "alias": f"DEPARTMENT_{slot}",
                    "canonical": "department",
                },
            )

        for alias in ("stakeholder", "stakeholders"):
            conn.execute(
                text(
                    """
                    INSERT INTO mem_mappings(namespace, alias, canonical, mapping_type, scope, source, confidence)
                    VALUES (:ns, :alias, 'stakeholder', 'term', 'global', 'dw_seed', 0.99)
                    ON CONFLICT (namespace, alias, mapping_type, scope) DO UPDATE
                      SET canonical = EXCLUDED.canonical,
                          confidence = EXCLUDED.confidence,
                          updated_at = NOW()
                    """
                ),
                {"ns": NAMESPACE, "alias": alias},
            )

        for alias in ("department", "departments"):
            conn.execute(
                text(
                    """
                    INSERT INTO mem_mappings(namespace, alias, canonical, mapping_type, scope, source, confidence)
                    VALUES (:ns, :alias, 'department', 'term', 'global', 'dw_seed', 0.95)
                    ON CONFLICT (namespace, alias, mapping_type, scope) DO UPDATE
                      SET canonical = EXCLUDED.canonical,
                          confidence = EXCLUDED.confidence,
                          updated_at = NOW()
                    """
                ),
                {"ns": NAMESPACE, "alias": alias},
            )

        seeded["mappings"].extend(
            [
                "stakeholder_columns",
                "department_columns",
                "stakeholder_terms",
                "department_terms",
            ]
        )

    return seeded


@dw_bp.route("/ingest", methods=["POST"])
def ingest():
    settings = Settings(namespace=NAMESPACE)
    mem_engine = settings.mem_engine()
    registry = DatasourceRegistry(settings, namespace=NAMESPACE)
    engine = registry.engine(None)

    inspector = inspect(engine)
    table_lookup = {name.upper(): name for name in inspector.get_table_names()}
    if "CONTRACT" not in table_lookup:
        return jsonify({"ok": False, "error": "Contract table not found in datasource."}), 400

    actual_name = table_lookup["CONTRACT"]
    columns = inspector.get_columns(actual_name)

    _ensure_mem_snapshot_schema(mem_engine)

    with mem_engine.begin() as conn:
        snapshot_id = conn.execute(
            text(
                """
                INSERT INTO mem_snapshots(namespace, schema_hash)
                VALUES (:ns, :hash)
                ON CONFLICT (namespace, schema_hash) DO UPDATE SET updated_at = NOW()
                RETURNING id
                """
            ),
            {"ns": NAMESPACE, "hash": "dw-oracle-contract-v1"},
        ).scalar_one()

        table_id = conn.execute(
            text(
                """
                INSERT INTO mem_tables(namespace, snapshot_id, table_name, schema_name, table_comment)
                VALUES (:ns, :sid, :tname, :sname, :comment)
                ON CONFLICT (namespace, table_name, schema_name)
                DO UPDATE SET snapshot_id = EXCLUDED.snapshot_id, updated_at = NOW()
                RETURNING id
                """
            ),
            {
                "ns": NAMESPACE,
                "sid": snapshot_id,
                "tname": actual_name,
                "sname": None,
                "comment": "DocuWare Contract base table",
            },
        ).scalar_one()

        for column in columns:
            conn.execute(
                text(
                    """
                    INSERT INTO mem_columns(namespace, table_id, column_name, data_type, is_nullable)
                    VALUES (:ns, :tid, :cname, :ctype, :nullable)
                    ON CONFLICT (namespace, table_id, column_name)
                    DO UPDATE SET data_type = EXCLUDED.data_type, updated_at = NOW()
                    """
                ),
                {
                    "ns": NAMESPACE,
                    "tid": table_id,
                    "cname": column.get("name"),
                    "ctype": str(column.get("type")),
                    "nullable": bool(column.get("nullable", True)),
                },
            )

        _manual_upsert_setting(
            conn,
            key="DW_CONTRACT_TABLE",
            value=actual_name,
            updated_by="dw_ingest",
        )
        _manual_upsert_setting(
            conn,
            key="DEFAULT_DATASOURCE",
            value="docuware",
            updated_by="dw_ingest",
        )
        _manual_upsert_setting(
            conn,
            key="RESEARCH_MODE",
            value=True,
            scope="namespace",
            updated_by="dw_ingest",
        )
        _manual_upsert_setting(
            conn,
            key="RESEARCHER_CLASS",
            value="apps.dw.research.DWResearcher",
            scope="global",
            updated_by="dw_ingest",
        )

    seeded = _seed_semantic_layer(mem_engine)

    return jsonify(
        {
            "ok": True,
            "namespace": NAMESPACE,
            "table": actual_name,
            "columns": len(columns),
            "seeded": seeded,
        }
    )


@dw_bp.route("/seed", methods=["POST"])
def seed():
    settings = Settings(namespace=NAMESPACE)
    mem_engine = settings.mem_engine()
    seeded = _seed_semantic_layer(mem_engine)
    return jsonify({"ok": True, "namespace": NAMESPACE, "seeded": seeded})


@dw_bp.route("/metrics", methods=["GET"])
def metrics():
    settings = Settings(namespace=NAMESPACE)
    mem_engine = settings.mem_engine()
    with mem_engine.begin() as conn:
        rows = conn.execute(
            text(
                """
                SELECT metric_key, metric_name, description, category, is_active, updated_at
                  FROM mem_metrics
                 WHERE namespace = :ns
              ORDER BY metric_key
                """
            ),
            {"ns": NAMESPACE},
        ).mappings().all()
    return jsonify(
        {
            "ok": True,
            "namespace": NAMESPACE,
            "metrics": [dict(row) for row in rows],
        }
    )


@dw_bp.route("/teach", methods=["POST"])
def teach():
    body = request.get_json(force=True) or {}
    glossary = body.get("glossary") or []
    mappings = body.get("mappings") or []
    metrics = body.get("metrics") or []
    rules = body.get("rules") or []
    qna = body.get("qna") or []

    settings = Settings()
    mem = get_mem_engine(settings)

    inserted = {"glossary": 0, "mappings": 0, "metrics": 0, "rules": 0, "qna": 0}
    with mem.begin() as conn:
        for item in glossary:
            conn.execute(
                text(
                    """
                INSERT INTO mem_glossary(namespace, term, definition, category, canonical_table, canonical_column, source, confidence)
                VALUES(:ns, :term, :def, :cat, :tbl, :col, 'user', 0.95)
                ON CONFLICT (namespace, term) DO UPDATE
                SET definition = EXCLUDED.definition, updated_at = NOW()
                """
                ),
                {
                    "ns": NAMESPACE,
                    "term": item["term"],
                    "def": item.get("definition"),
                    "cat": item.get("category"),
                    "tbl": item.get("canonical_table"),
                    "col": item.get("canonical_column"),
                },
            )
            inserted["glossary"] += 1

        for item in mappings:
            conn.execute(
                text(
                    """
                INSERT INTO mem_mappings(namespace, alias, canonical, mapping_type, scope, source, confidence)
                VALUES(:ns, :alias, :canonical, :type, COALESCE(:scope,'global'), 'user', COALESCE(:conf,0.9))
                ON CONFLICT (namespace, alias, mapping_type, scope) DO UPDATE
                SET canonical = EXCLUDED.canonical, updated_at = NOW()
                """
                ),
                {
                    "ns": NAMESPACE,
                    "alias": item["alias"],
                    "canonical": item["canonical"],
                    "type": item.get("mapping_type", "term"),
                    "scope": item.get("scope"),
                    "conf": item.get("confidence"),
                },
            )
            inserted["mappings"] += 1

        for metric in metrics:
            conn.execute(
                text(
                    """
                INSERT INTO mem_metrics(namespace, metric_key, metric_name, description, calculation_sql, category, owner, is_active)
                VALUES(:ns, :k, :name, :desc, :sql, :cat, 'dw', true)
                ON CONFLICT (namespace, metric_key, version) DO UPDATE
                SET calculation_sql = EXCLUDED.calculation_sql, description = EXCLUDED.description, updated_at = NOW()
                """
                ),
                {
                    "ns": NAMESPACE,
                    "k": metric["metric_key"],
                    "name": metric.get("metric_name"),
                    "desc": metric.get("description"),
                    "sql": metric["calculation_sql"],
                    "cat": metric.get("category", "contracts"),
                },
            )
            inserted["metrics"] += 1

        for rule in rules:
            conn.execute(
                text(
                    """
                INSERT INTO mem_rules(namespace, rule_name, rule_type, scope, condition_sql, description, priority, is_mandatory, source, confidence)
                VALUES(:ns,:name,:type,COALESCE(:scope,'global'),:cond,:desc,COALESCE(:prio,100),COALESCE(:mand,false),'user',COALESCE(:conf,0.9))
                ON CONFLICT (namespace, rule_name) DO UPDATE
                SET condition_sql = EXCLUDED.condition_sql, description = EXCLUDED.description, updated_at = NOW()
                """
                ),
                {
                    "ns": NAMESPACE,
                    "name": rule["rule_name"],
                    "type": rule.get("rule_type", "filter"),
                    "scope": rule.get("scope"),
                    "cond": rule.get("condition_sql"),
                    "desc": rule.get("description"),
                    "prio": rule.get("priority"),
                    "mand": rule.get("is_mandatory"),
                    "conf": rule.get("confidence"),
                },
            )
            inserted["rules"] += 1

        for example in qna:
            conn.execute(
                text(
                    """
                INSERT INTO mem_qna(namespace, question, answer, context, question_type, created_at)
                VALUES(:ns,:q,:a,:ctx,:qt,NOW())
                """
                ),
                {
                    "ns": NAMESPACE,
                    "q": example["question"],
                    "a": example["answer"],
                    "ctx": example.get("context"),
                    "qt": example.get("question_type"),
                },
            )
            inserted["qna"] += 1

    return jsonify({"ok": True, "inserted": inserted})


@dw_bp.route("/answer", methods=["POST"])
def answer():
    data = request.get_json(force=True) or {}
    question = (data.get("question") or "").strip()
    prefixes = list(data.get("prefixes") or [])
    auth_email = data.get("auth_email") or None
    datasource = data.get("datasource")

    if not question:
        return jsonify({"ok": False, "error": "question is required"}), 400

    wants_window = _wants_time_window(question)

    settings = Settings(namespace=NAMESPACE)
    ds_registry = DatasourceRegistry(settings, namespace=NAMESPACE)
    try:
        oracle = ds_registry.engine(datasource)
    except Exception as exc:
        return jsonify({"ok": False, "error": f"no datasource engine: {exc}"}), 500

    mem_engine = settings.mem_engine()
    inquiry_id: Optional[int] = None
    if mem_engine is not None:
        try:
            inquiry_id = _insert_inquiry(
                mem_engine,
                NAMESPACE,
                question,
                auth_email,
                prefixes,
                "open",
            )
        except Exception:
            inquiry_id = None

    def _update_inquiry(status: str) -> None:
        if mem_engine is None or inquiry_id is None:
            return
        try:
            with mem_engine.begin() as conn:
                conn.execute(
                    text(
                        """
                        UPDATE mem_inquiries
                           SET status = :st, updated_at = NOW()
                         WHERE id = :id
                        """
                    ),
                    {"st": status, "id": inquiry_id},
                )
        except Exception:
            pass

    def _needs_clarification(
        reason: Optional[str] = None,
        *,
        sql_text: Optional[str] = None,
        error: Optional[str] = None,
        status: str = "needs_clarification",
    ):
        _update_inquiry(status)
        followups = propose_clarifying_questions(question)
        if reason and reason not in followups:
            followups = [reason] + followups
        payload = {
            "ok": False,
            "status": status,
            "questions": followups,
            "inquiry_id": inquiry_id,
        }
        if sql_text:
            payload["sql"] = sql_text
        if error:
            payload["error"] = error
        return jsonify(payload)

    def _normalize_dt(value: Any) -> Any:
        if isinstance(value, datetime):
            if value.tzinfo is not None:
                return value.replace(tzinfo=None)
            return value
        return value

    sql, gen_meta = nl_to_sql_with_llm(question)
    gen_meta = gen_meta or {}

    if not sql:
        _update_inquiry("needs_clarification")
        return jsonify(
            {
                "ok": False,
                "status": "needs_clarification",
                "inquiry_id": inquiry_id,
                "error": gen_meta.get("reason", "not_select"),
                "questions": [
                    "I couldn't derive a clean SELECT. Can you rephrase or specify filters (stakeholders, departments, date columns)?"
                ],
                "sql": None,
            }
        )

    sql = sql.strip()

    if not re.match(r"^\s*(WITH|SELECT)\b", sql, flags=re.IGNORECASE):
        _update_inquiry("needs_clarification")
        return jsonify(
            {
                "ok": False,
                "status": "needs_clarification",
                "inquiry_id": inquiry_id,
                "error": "not_select",
                "questions": [
                    "Please confirm you want a SELECT-only query; no updates/deletes allowed.",
                ],
                "sql": sql,
            }
        )

    bad_binds = _unexpected_binds(sql)
    if bad_binds:
        _update_inquiry("needs_clarification")
        return jsonify(
            {
                "ok": False,
                "status": "needs_clarification",
                "inquiry_id": inquiry_id,
                "error": "unexpected_binds",
                "questions": [
                    f"The model produced bind(s) {sorted(bad_binds)} which we don't support. Rephrase without those binds, or specify an explicit date window on a named date column to use :date_start/:date_end."
                ],
                "sql": sql,
            }
        )

    if _has_dml(sql):
        return _needs_clarification(
            "I drafted SQL that didn't look like a safe SELECT. Could you clarify?",
            sql_text=sql,
            error="not_select",
        )

    bind_names = _find_named_binds(sql)
    params: Dict[str, Any] = {}

    if {"date_start", "date_end"} & bind_names:
        if not wants_window:
            _update_inquiry("needs_clarification")
            return jsonify(
                {
                    "ok": False,
                    "status": "needs_clarification",
                    "inquiry_id": inquiry_id,
                    "error": "missing_date_context",
                    "questions": [
                        "Your query expects a date window (:date_start/:date_end). Which date column and what window should we use (e.g., END_DATE next 30 days)?"
                    ],
                    "sql": sql,
                }
            )

        window_params = _compute_window(question, now=datetime.now(timezone.utc))
        if "date_start" not in window_params or "date_end" not in window_params:
            _update_inquiry("needs_clarification")
            return jsonify(
                {
                    "ok": False,
                    "status": "needs_clarification",
                    "inquiry_id": inquiry_id,
                    "error": "date_parse_failed",
                    "questions": [
                        "We couldn't parse the requested time window. Please specify explicit dates or a relative window on a named date column."
                    ],
                    "sql": sql,
                }
            )

        params["date_start"] = _normalize_dt(window_params["date_start"])
        params["date_end"] = _normalize_dt(window_params["date_end"])

    expected_binds = set(_binds_required_in_sql(sql))
    missing_binds = sorted(name for name in expected_binds if name not in params)
    if missing_binds:
        return _needs_clarification(
            f"Provide values for: {', '.join(missing_binds)} or rephrase with explicit dates.",
            sql_text=sql,
            error="missing_binds",
        )

    exec_binds = {name: params[name] for name in expected_binds if name in params}

    appended_limit = False
    if "fetch first" not in sql.lower():
        sql = f"{sql.rstrip(';')}\nFETCH FIRST 500 ROWS ONLY"
        appended_limit = True

    start_time = time.perf_counter()
    ok, fetched_rows, meta_exec, exec_error = run_sql_oracle(oracle, sql, exec_binds)
    elapsed_ms = int((time.perf_counter() - start_time) * 1000)
    if not ok:
        return _needs_clarification(
            "The generated SQL did not run. Could you clarify the filters or timeframe?",
            sql_text=sql,
            error=exec_error,
            status="failed",
        )

    rows = fetched_rows or []
    meta_payload: Dict[str, Any] = dict(meta_exec or {})
    serialized_binds: Dict[str, Any] = {}
    for key, value in exec_binds.items():
        if isinstance(value, (datetime, date)):
            serialized_binds[key] = value.isoformat()
        else:
            serialized_binds[key] = value
    meta_payload["binds"] = serialized_binds
    meta_payload["clarifier_used"] = False
    meta_payload["limit_applied"] = appended_limit
    meta_payload["elapsed_ms"] = elapsed_ms
    meta_payload["rewritten_question"] = question
    meta_payload["llm_reason"] = gen_meta.get("reason", "ok")
    meta_payload["llm_retries"] = gen_meta.get("retries", 0)
    if "confidence" in gen_meta:
        meta_payload["llm_confidence"] = gen_meta.get("confidence")
    if "unexpected_binds_first_try" in gen_meta:
        meta_payload["llm_unexpected_binds_first_try"] = gen_meta.get("unexpected_binds_first_try")
    meta_payload["generation_mode"] = "llm"
    meta_payload.setdefault("rowcount", len(rows))
    if "columns" not in meta_payload and rows:
        meta_payload["columns"] = list(rows[0].keys())

    tags = ["dw", "contracts", "llm"]
    intent_payload = None
    run_id = save_learning_artifacts(
        NAMESPACE,
        question,
        sql,
        rows,
        intent=intent_payload,
        tags=tags,
        settings=settings,
        date_column=None,
        window_label=None,
    )

    if mem_engine is not None:
        try:
            with mem_engine.begin() as conn:
                save_success_snippet(conn, NAMESPACE, question, sql, tags)
                _learn_mappings(conn)
        except Exception:
            pass

    autosave = settings.get_bool("SNIPPETS_AUTOSAVE", default=False, scope="namespace") or False
    if autosave and rows and mem_engine is not None:
        try:
            with mem_engine.begin() as conn:
                conn.execute(
                    text(
                        """
                        INSERT INTO mem_snippets(namespace, title, description, sql_template, sql_raw,
                                                 input_tables, output_columns, parameters, tags, is_verified, created_at, updated_at)
                        VALUES (:ns, :title, :desc, :tmpl, :raw, '["Contract"]'::jsonb, CAST(:cols AS jsonb), CAST(:params AS jsonb),
                                '["dw","contracts"]'::jsonb, true, NOW(), NOW())
                        """
                    ),
                    {
                        "ns": NAMESPACE,
                        "title": question[:200],
                        "desc": "Saved from successful /dw/answer",
                        "tmpl": sql,
                        "raw": sql,
                        "cols": json.dumps(meta_payload.get("columns", [])),
                        "params": json.dumps(serialized_binds),
                    },
                )
        except Exception:
            pass

    _update_inquiry("complete")

    csv_payload = _csv_bytes(rows)
    os.makedirs("/tmp/exports", exist_ok=True)
    export_id = run_id if run_id is not None else int(datetime.now().timestamp())
    csv_path = f"/tmp/exports/dw_run_{export_id}.csv"
    with open(csv_path, "wb") as handle:
        handle.write(csv_payload)

    response_payload: Dict[str, Any] = {
        "ok": True,
        "rows": rows[:100],
        "sql": sql,
        "csv_path": csv_path,
        "meta": meta_payload,
        "inquiry_id": inquiry_id,
    }

    if rows_suspiciously_empty(rows, question):
        response_payload["hint"] = (
            "No rows matched. Try adjusting the filters or timeframe."
        )

    return jsonify(response_payload)
