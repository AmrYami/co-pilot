from __future__ import annotations

import csv
import io
import json
import os
import re
import time
from datetime import date, datetime, timedelta
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from flask import Blueprint, jsonify, request
from sqlalchemy import create_engine, inspect, text

from core.datasources import DatasourceRegistry
from core.settings import Settings
from core.sql_exec import get_mem_engine

from apps.dw.clarifier import propose_clarifying_questions
from apps.dw.llm import nl_to_sql_with_llm

if TYPE_CHECKING:  # pragma: no cover
    from core.pipeline import Pipeline


NAMESPACE = "dw::common"
dw_bp = Blueprint("dw", __name__)


DW_BIND_WHITELIST = {
    "date_start",
    "date_end",
    "top_n",
    "owner_name",
    "dept",
    "entity_no",
    "contract_id_pattern",
    "request_type",
}

_BIND_RE = re.compile(r":([A-Za-z_][A-Za-z0-9_]*)")


def _binds_used_in_sql(sql: str) -> set[str]:
    if not sql:
        return set()
    return set(_BIND_RE.findall(sql))


def _validate_bind_whitelist(sql: str, whitelist: set[str]) -> tuple[bool, list[str]]:
    used = _binds_used_in_sql(sql)
    not_allowed = sorted(used - whitelist)
    return (len(not_allowed) == 0, not_allowed)


def _filter_binds_for_sql(sql: str, candidate_binds: dict) -> dict:
    used = _binds_used_in_sql(sql)
    return {k: v for k, v in (candidate_binds or {}).items() if k in used}


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


def _infer_window_from_question(q: str) -> dict | None:
    """Return {'start': date, 'end': date, 'label': '...'} or None if no explicit window."""

    if not q:
        return None
    raw = q.strip()
    lowered = raw.lower()
    today = datetime.utcnow().date()

    def _parse_date_token(token: str) -> date | None:
        try:
            return date.fromisoformat(token)
        except ValueError:
            return None

    if re.search(r"\blast\s+month\b", lowered):
        first_this = today.replace(day=1)
        prev_month_last_day = first_this - timedelta(days=1)
        last_start = prev_month_last_day.replace(day=1)
        return {"start": last_start, "end": first_this, "label": "last month"}

    if re.search(r"\bnext\s+month\b", lowered):
        first_this = today.replace(day=1)
        if first_this.month == 12:
            first_next = first_this.replace(year=first_this.year + 1, month=1)
        else:
            first_next = first_this.replace(month=first_this.month + 1)
        if first_next.month == 12:
            first_after = first_next.replace(year=first_next.year + 1, month=1)
        else:
            first_after = first_next.replace(month=first_next.month + 1)
        return {"start": first_next, "end": first_after, "label": "next month"}

    match = re.search(r"\b(next|last|past|coming)\s+(\d+)\s+days\b", lowered)
    if match:
        keyword = match.group(1)
        days = int(match.group(2))
        if keyword in {"next", "coming"}:
            return {
                "start": today,
                "end": today + timedelta(days=days),
                "label": f"next {days} days",
            }
        return {
            "start": today - timedelta(days=days),
            "end": today + timedelta(days=1),
            "label": f"last {days} days",
        }

    direct = re.search(
        r"\b(?:between|from)\s+(\d{4}-\d{2}-\d{2})\s+(?:and|to)\s+(\d{4}-\d{2}-\d{2})",
        raw,
        re.IGNORECASE,
    )
    if direct:
        start_token = direct.group(1)
        end_token = direct.group(2)
        start_date = _parse_date_token(start_token)
        end_date = _parse_date_token(end_token)
        if start_date and end_date:
            if end_date < start_date:
                start_date, end_date = end_date, start_date
            return {
                "start": start_date,
                "end": end_date + timedelta(days=1),
                "label": f"{start_token} to {end_token}",
            }

    since = re.search(r"\bsince\s+(\d{4}-\d{2}-\d{2})", raw, re.IGNORECASE)
    if since:
        start_token = since.group(1)
        start_date = _parse_date_token(start_token)
        if start_date:
            return {
                "start": start_date,
                "end": today + timedelta(days=1),
                "label": f"since {start_token}",
            }

    return None


def _choose_date_column(q: str) -> str | None:
    """Pick the date column named by the user, else None (no implicit default)."""

    s = (q or "").lower()
    if "end date" in s or "expiry" in s or "expires" in s:
        return "END_DATE"
    if "start date" in s:
        return "START_DATE"
    if "request date" in s:
        return "REQUEST_DATE"
    return None


_WINDOW_PATTERNS = [
    r"(?i)\bnext\s+\d+\s+(day|days|week|weeks|month|months|quarter|quarters|year|years)\b",
    r"(?i)\blast\s+(month|quarter|year|week|7\s*days|30\s*days|90\s*days)\b",
    r"(?i)\bbetween\s+\d{4}-\d{2}-\d{2}\s+and\s+\d{4}-\d{2}-\d{2}\b",
    r"(?i)\bsince\s+\d{4}-\d{2}-\d{2}\b",
    r"(?i)\bthis\s+(month|quarter|year)\b",
    r"(?i)\btoday\b|\btomorrow\b|\byesterday\b",
]


def _window_requested(text: str) -> bool:
    t = (text or "").strip()
    for pat in _WINDOW_PATTERNS:
        if re.search(pat, t):
            return True
    return False


def _validate_sql(
    sql: str,
    *,
    need_window: bool,
    date_col_hint: str | None,
) -> tuple[bool, str, set[str]]:
    normalized = (sql or "").strip()
    if not normalized:
        return False, "empty_sql", set()
    lowered = normalized.lstrip().lower()
    if not (lowered.startswith("select") or lowered.startswith("with")):
        return False, "not_select", set()

    binds = _binds_used_in_sql(sql)

    has_window_binds = {"date_start", "date_end"}.issubset(binds)
    any_window_binds = bool({"date_start", "date_end"} & binds)

    if need_window:
        if not has_window_binds:
            return False, "missing_date_context", binds
    else:
        if any_window_binds and not date_col_hint:
            return False, "unexpected_date_filter", binds

    return True, "ok", binds


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

    date_col_hint = _choose_date_column(question)
    need_window = _window_requested(question)

    sql = nl_to_sql_with_llm(question, context={})
    if not sql:
        return _needs_clarification(
            "I couldn't derive a clean SELECT. Could you rephrase or specify filters (stakeholders, departments, date columns)?"
        )

    candidate_binds: Dict[str, Any] = {}
    window_label: Optional[str] = None
    date_column_used: Optional[str] = None
    win = _infer_window_from_question(question) if need_window else None
    if win:
        date_col = date_col_hint or "REQUEST_DATE"
        start_dt = _normalize_dt(win["start"]) if "start" in win else None
        end_dt = _normalize_dt(win["end"]) if "end" in win else None
        if start_dt and end_dt:
            candidate_binds["date_start"] = start_dt
            candidate_binds["date_end"] = end_dt
            window_label = win.get("label")
            date_column_used = date_col

    ok_sql, reason, _ = _validate_sql(
        sql,
        need_window=need_window,
        date_col_hint=date_col_hint,
    )
    if not ok_sql:
        if reason == "missing_date_context":
            return _needs_clarification(
                "This question needs a start and end date. Please provide the timeframe you have in mind.",
                sql_text=sql,
                error=reason,
            )
        if reason == "unexpected_date_filter":
            return _needs_clarification(
                "No timeframe was requested, but the SQL added date filters. Please clarify the desired dates or rephrase without them.",
                sql_text=sql,
                error=reason,
            )
        return _needs_clarification(
            "I couldn't derive a clean SELECT. Could you rephrase or specify filters?",
            sql_text=sql,
            error=reason,
        )

    ok_allowed, not_allowed = _validate_bind_whitelist(sql, DW_BIND_WHITELIST)
    if not ok_allowed:
        _update_inquiry("needs_clarification")
        payload = {
            "ok": False,
            "status": "needs_clarification",
            "error": "binds_not_allowed",
            "details": {"not_allowed": not_allowed},
            "sql": sql,
            "inquiry_id": inquiry_id,
        }
        return jsonify(payload), 200

    candidate_binds = dict(candidate_binds or {})
    runtime_binds = _filter_binds_for_sql(sql, candidate_binds)

    used_binds = _binds_used_in_sql(sql)
    missing = sorted(used_binds - set(runtime_binds.keys()))
    if missing:
        _update_inquiry("needs_clarification")
        payload = {
            "ok": False,
            "status": "needs_clarification",
            "error": "missing_binds",
            "questions": [
                "Provide values for: " + ", ".join(missing) + " or rephrase with explicit filters."
            ],
            "sql": sql,
            "inquiry_id": inquiry_id,
        }
        return jsonify(payload), 200

    appended_limit = False
    if "fetch first" not in sql.lower():
        sql = f"{sql.rstrip(';')}\nFETCH FIRST 500 ROWS ONLY"
        appended_limit = True

    start_time = time.perf_counter()
    ok, fetched_rows, meta_exec, exec_error = run_sql_oracle(oracle, sql, runtime_binds)
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
    for key, value in runtime_binds.items():
        if isinstance(value, (datetime, date)):
            serialized_binds[key] = value.isoformat()
        else:
            serialized_binds[key] = value
    meta_payload["binds"] = serialized_binds
    meta_payload["clarifier_used"] = False
    meta_payload["limit_applied"] = appended_limit
    meta_payload["elapsed_ms"] = elapsed_ms
    meta_payload["rewritten_question"] = question
    meta_payload["llm_reason"] = "model_sql"
    meta_payload["llm_retries"] = 0
    meta_payload["generation_mode"] = "llm"
    if window_label:
        meta_payload["window_label"] = window_label
    if date_column_used:
        meta_payload["date_column"] = date_column_used
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
        date_column=date_column_used,
        window_label=window_label,
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
