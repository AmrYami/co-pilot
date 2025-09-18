from __future__ import annotations

import csv
import io
import json
import os
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING

from flask import Blueprint, jsonify, request
from sqlalchemy import create_engine, inspect, text

from core.datasources import DatasourceRegistry
from core.settings import Settings
from core.sql_exec import get_mem_engine

from .llm import DWLLM

if TYPE_CHECKING:  # pragma: no cover
    from core.pipeline import Pipeline


NAMESPACE = "dw::common"
dw_bp = Blueprint("dw", __name__)


@dw_bp.route("/model/info", methods=["GET"])
def model_info():
    info = {
        "clarifier": "disabled",
        "llm": "unknown",
        "mode": "dw-pipeline",
    }
    try:  # pragma: no cover - health check
        settings = Settings(namespace=NAMESPACE)
        DWLLM(settings).nl_to_sql("ping")
        info["llm"] = "available"
    except Exception:
        info["llm"] = "unavailable"
    return jsonify(info)


def create_dw_blueprint(settings: Settings | None = None, pipeline: "Pipeline | None" = None) -> Blueprint:
    """Return the DocuWare blueprint wired to the provided pipeline/settings."""
    if settings is not None:
        try:
            settings.set_namespace(NAMESPACE)
        except AttributeError:
            pass
    return dw_bp


def _pg(conn_str: str):
    return create_engine(conn_str, pool_pre_ping=True, future=True)


def _calc_window(question: str) -> Tuple[date, date, int]:
    now = date.today()
    q = question.lower()
    top_n = 10
    if "top " in q:
        try:
            toks = q.split()
            idx = toks.index("top")
            top_n = int(toks[idx + 1])
        except Exception:
            pass
    if "last month" in q:
        first_this = now.replace(day=1)
        last_month_end = first_this
        last_month_start = (first_this - timedelta(days=1)).replace(day=1)
        return last_month_start, last_month_end, top_n
    if "last 90 days" in q:
        return now - timedelta(days=90), now + timedelta(days=1), top_n
    if "next 30 days" in q or "in the next 30 days" in q:
        return now, now + timedelta(days=30), top_n
    return now - timedelta(days=30), now + timedelta(days=1), top_n


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


def _learn_snippet(conn, question: str, sql: str, tags):
    conn.execute(
        text(
            """
        INSERT INTO mem_snippets(namespace, title, description, sql_raw, input_tables, tags, is_verified, source)
        VALUES (:ns, :title, :desc, :sql, '["Contract"]'::jsonb, :tags::jsonb, false, 'dw')
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

    settings = Settings(namespace=NAMESPACE)
    ds_registry = DatasourceRegistry(settings, namespace=NAMESPACE)
    try:
        oracle = ds_registry.engine(datasource)
    except Exception as exc:
        return jsonify({"ok": False, "error": f"no datasource engine: {exc}"}), 500

    mem = _pg(settings.get("MEMORY_DB_URL", scope="global"))

    start, end, top_n = _calc_window(question)
    binds = {
        "date_start": datetime.combine(start, datetime.min.time()),
        "date_end": datetime.combine(end, datetime.min.time()),
        "top_n": top_n,
    }

    llm = DWLLM(settings)
    try:
        sql = llm.nl_to_sql(question)
    except Exception as exc:
        inquiry_id = _insert_inquiry(
            mem,
            NAMESPACE,
            question,
            auth_email,
            prefixes,
            "needs_clarification",
        )
        return jsonify(
            {
                "ok": False,
                "status": "needs_clarification",
                "questions": [
                    "I couldn't derive a clean SQL. Can you clarify the columns or filters?"
                ],
                "error": f"nl_to_sql failed: {exc}",
                "inquiry_id": inquiry_id,
            }
        )

    rows: List[Dict[str, Any]] = []
    try:
        with oracle.connect() as conn:
            result = conn.execute(text(sql), binds)
            columns = result.keys()
            for record in result.fetchall():
                rows.append({col: record[idx] for idx, col in enumerate(columns)})
    except Exception as exc:
        inquiry_id = _insert_inquiry(
            mem,
            NAMESPACE,
            question,
            auth_email,
            prefixes,
            "needs_clarification",
        )
        return jsonify(
            {
                "ok": False,
                "status": "needs_clarification",
                "error": f"SQL execution error: {exc}",
                "sql": sql,
                "inquiry_id": inquiry_id,
            }
        )

    run_id = _insert_run(
        mem,
        NAMESPACE,
        auth_email or "dw_user",
        question,
        sql,
        "complete",
        len(rows),
        rows[:20],
    )

    meta = {
        "date_start": binds["date_start"].isoformat(),
        "date_end": binds["date_end"].isoformat(),
        "top_n": top_n,
    }

    if not rows:
        return jsonify(
            {
                "ok": True,
                "rows": [],
                "sql": sql,
                "hint": "No results for that window. Try 'last 90 days' or specify START_DATE/END_DATE filters.",
                "meta": meta,
            }
        )

    csv_payload = _csv_bytes(rows)
    os.makedirs("/tmp/exports", exist_ok=True)
    csv_path = f"/tmp/exports/dw_run_{run_id}.csv"
    with open(csv_path, "wb") as handle:
        handle.write(csv_payload)

    with mem.begin() as conn:
        _learn_snippet(conn, question, sql, tags=["dw", "docuware", "contract"])
        _learn_mappings(conn)

    return jsonify(
        {
            "ok": True,
            "rows": rows[:100],
            "sql": sql,
            "csv_path": csv_path,
            "meta": meta,
        }
    )
