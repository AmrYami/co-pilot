import json
import os
import re
from typing import Any, Dict, List

try:  # pragma: no cover - allow tests to import without Flask installed
    from flask import Blueprint, current_app, jsonify, request
except Exception:  # pragma: no cover - simple stub used in unit tests
    current_app = None  # type: ignore[assignment]

    class _StubBlueprint:  # minimal methods to satisfy imports
        def __init__(self, *args, **kwargs):
            pass

        def register_blueprint(self, *args, **kwargs):
            return None

        def post(self, *args, **kwargs):
            def decorator(fn):
                return fn

            return decorator

    def _jsonify(*args, **kwargs):  # pragma: no cover - test stub
        return {}

    class _StubRequest:  # pragma: no cover - test stub
        args: Dict[str, str] = {}

        def get_json(self, force: bool = False):  # noqa: D401 - simple stub
            return {}

    Blueprint = _StubBlueprint  # type: ignore[assignment]
    jsonify = _jsonify  # type: ignore[assignment]
    request = _StubRequest()  # type: ignore[assignment]

try:  # pragma: no cover - optional dependency in tests
    from sqlalchemy import bindparam, text
    from sqlalchemy.types import Date, Integer
except Exception:  # pragma: no cover - lightweight fallback used in tests
    class _StubStatement:  # minimal stand-in when SQLAlchemy is absent
        def __init__(self, sql: str):
            self.sql = sql

        def bindparams(self, *args, **kwargs):
            return self

    def text(sql: str):  # type: ignore
        return _StubStatement(sql)

    def bindparam(key, type_=None):  # type: ignore
        return {"key": key, "type": type_}

    class Date:  # type: ignore
        def __call__(self, *args, **kwargs):
            return self

    class Integer(Date):  # type: ignore
        pass

from core.settings import Settings

from .engine.clarify import parse_intent
from .engine.build_sql import build_sql
from .engine.explain import build_explain
from .engine.fts import build_fts_where
from .engine.table_profiles import CONTRACT_TABLE, fts_columns
from .rating import rate_bp
from .dates import coerce_oracle_date

NAMESPACE = os.getenv("DW_NAMESPACE", "dw::common")


dw_bp = Blueprint("dw", __name__)
dw_bp.register_blueprint(rate_bp, url_prefix="")


def _resolve_dw_engine(app):
    engine = app.config.get("DW_ENGINE") if app else None  # type: ignore[assignment]
    if engine is not None:
        return engine
    pipeline = app.config.get("PIPELINE") if app else None
    if pipeline is None:
        pipeline = app.config.get("pipeline") if app else None
    if pipeline is None:
        return None
    try:
        return pipeline.ds.engine(None)  # type: ignore[attr-defined]
    except Exception:  # pragma: no cover - defensive fallback
        return getattr(pipeline, "app_engine", None)


def _execute_oracle(sql: str, binds: Dict[str, Any]):
    app = current_app
    rows: List[List[Any]] = []
    cols: List[str] = []
    meta: Dict[str, Any] = {
        "validation": {"ok": True, "errors": [], "binds": list(binds.keys())},
        "csv_path": None,
    }
    engine = _resolve_dw_engine(app)
    if engine is None:
        return rows, cols, meta
    with engine.connect() as cx:  # type: ignore[union-attr]
        coerced_binds = dict(binds or {})
        if "date_start" in coerced_binds:
            coerced_binds["date_start"] = coerce_oracle_date(coerced_binds.get("date_start"))
        if "date_end" in coerced_binds:
            coerced_binds["date_end"] = coerce_oracle_date(coerced_binds.get("date_end"))
        if "top_n" in coerced_binds and coerced_binds["top_n"] is not None:
            coerced_binds["top_n"] = int(coerced_binds["top_n"])

        stmt = text(sql)
        if ":date_start" in sql:
            stmt = stmt.bindparams(bindparam("date_start", type_=Date()))
        if ":date_end" in sql:
            stmt = stmt.bindparams(bindparam("date_end", type_=Date()))
        if ":top_n" in sql:
            stmt = stmt.bindparams(bindparam("top_n", type_=Integer()))

        rs = cx.execute(stmt, coerced_binds)
        cols = list(rs.keys()) if hasattr(rs, "keys") else []
        rows = [list(r) for r in rs.fetchall()]
    meta["rowcount"] = len(rows)
    return rows, cols, meta


def _intent_debug(intent) -> Dict[str, Any]:
    if hasattr(intent, "model_dump"):
        data = intent.model_dump()
    elif isinstance(intent, dict):
        data = dict(intent)
    else:
        data = {attr: getattr(intent, attr) for attr in dir(intent) if not attr.startswith("_")}
    keep = {
        "agg",
        "date_column",
        "expire",
        "explicit_dates",
        "full_text_search",
        "fts_tokens",
        "group_by",
        "has_time_window",
        "measure_sql",
        "sort_by",
        "sort_desc",
        "top_n",
        "user_requested_top_n",
        "wants_all_columns",
    }
    return {k: data.get(k) for k in keep}


def _coerce_prefixes(raw) -> List[str]:
    if not raw:
        return []
    if isinstance(raw, (list, tuple, set)):
        return [str(p) for p in raw if p is not None]
    return [str(raw)]


def _tokenize_fts(text_value: str) -> List[str]:
    lowered = (text_value or "").lower()
    return [tok for tok in re.findall(r"[a-z0-9']+", lowered) if tok]


def _format_window_val(val: Any) -> str:
    if hasattr(val, "strftime"):
        try:
            return val.strftime("%Y-%m-%d")
        except Exception:
            return str(val)
    return str(val)


def _build_user_explain(intent: Any, binds: Dict[str, Any], sql: str) -> str:
    parts: List[str] = []
    _ = sql  # placeholder to document the SQL analyzed
    binds = binds or {}
    ds = binds.get("date_start")
    de = binds.get("date_end")
    if ds and de:
        parts.append(
            f"Interpreted the requested window from {_format_window_val(ds)} to {_format_window_val(de)}."
        )

    dc = getattr(intent, "date_column", None)
    if dc == "OVERLAP":
        parts.append(
            "Treated a contract as active when the START_DATE/END_DATE overlap the requested window."
        )
    elif dc == "REQUEST_DATE":
        parts.append("Used REQUEST_DATE because the question refers to request timing.")
    elif dc == "END_DATE":
        parts.append("Used END_DATE as specified in the question.")

    gb = getattr(intent, "group_by", None)
    agg = getattr(intent, "agg", None)
    if gb and agg:
        parts.append(f"Grouped results by {gb} using {str(agg).upper()}.")
    elif gb:
        parts.append(f"Grouped results by {gb}.")

    sort_by = getattr(intent, "sort_by", None)
    if sort_by:
        desc = getattr(intent, "sort_desc", True)
        order_word = "descending" if desc else "ascending"
        parts.append(f"Sorted the results in {order_word} order by the requested measure.")

    if getattr(intent, "wants_all_columns", False):
        parts.append("All columns were returned because none were specifically requested.")

    if getattr(intent, "full_text_search", False):
        parts.append("Full-text search was enabled across the configured FTS columns.")

    return " ".join(parts) if parts else "Used default interpretation settings for your question."


@dw_bp.post("/answer")
def answer():
    app = current_app
    data = request.get_json(force=True) or {}
    question = (data.get("question") or "").strip()
    prefixes = _coerce_prefixes(data.get("prefixes"))
    auth_email = (data.get("auth_email") or "").strip()
    namespace = (data.get("namespace") or NAMESPACE).strip() or NAMESPACE
    full_text_search = bool(data.get("full_text_search"))
    include_explain_req = data.get("include_explain")
    explain_flag = data.get("explain")

    if not question:
        return jsonify({"ok": False, "error": "question required"}), 400

    pipeline = app.config.get("PIPELINE") or app.config.get("pipeline")
    settings = getattr(pipeline, "settings", None) if pipeline else None
    if settings is None:
        settings = app.config.get("SETTINGS") or Settings(namespace=namespace)

    mem_engine = app.config.get("MEM_ENGINE") or getattr(pipeline, "mem_engine", None)
    if mem_engine is None:
        return jsonify({"ok": False, "error": "mem_engine_unavailable"}), 503

    with mem_engine.begin() as cx:
        row = cx.execute(
            text(
                """
            INSERT INTO mem_inquiries(namespace, prefixes, question, auth_email, status, created_at, updated_at)
            VALUES(:ns, :pfx, :q, :mail, 'open', NOW(), NOW())
            RETURNING id
        """
            ),
            {
                "ns": namespace,
                "pfx": json.dumps(prefixes),
                "q": question,
                "mail": auth_email,
            },
        ).fetchone()
    inquiry_id = int(row[0]) if row else None

    strict_overlap = bool(int(settings.get("DW_OVERLAP_STRICT", "1") or 1))

    intent = parse_intent(question)
    intent.full_text_search = full_text_search
    intent.explain_on = True if explain_flag is None else bool(explain_flag)
    if not intent.measure_sql:
        from .engine.table_profiles import net_sql  # local import to avoid cycle

        intent.measure_sql = net_sql()

    fts_meta: Dict[str, Any] = {
        "enabled": full_text_search,
        "tokens": None,
        "columns": None,
        "binds": None,
        "error": None,
    }
    fts_where = ""
    fts_bind: Dict[str, Any] = {}
    if full_text_search:
        try:
            columns = fts_columns(settings, table_name=CONTRACT_TABLE)
            tokens = intent.fts_tokens or _tokenize_fts(question)
            intent.fts_tokens = tokens
            fts_meta.update({"columns": columns, "tokens": tokens})
            if columns and tokens:
                fts_where, fts_bind = build_fts_where(tokens, columns)
        except Exception as exc:  # pragma: no cover - defensive log only
            fts_meta["error"] = str(exc)

    sql, binds = build_sql(intent, strict_overlap=strict_overlap)

    if fts_where:
        upper_sql = sql.upper()
        order_idx = upper_sql.find("ORDER BY")
        if order_idx >= 0:
            prefix = sql[:order_idx]
            suffix = sql[order_idx:]
        else:
            prefix = sql
            suffix = ""
        if "WHERE" in prefix.upper():
            prefix = prefix.rstrip() + f"\n  AND {fts_where}\n"
        else:
            prefix = prefix.rstrip() + f"\nWHERE {fts_where}\n"
        sql = prefix + suffix
        binds.update(fts_bind)

    rows, cols, exec_meta = _execute_oracle(sql, binds)

    fts_meta["binds"] = list(fts_bind.keys()) or None

    debug = {
        "intent": _intent_debug(intent),
        "validation": exec_meta.get("validation", {}),
        "fts": fts_meta,
    }

    include_explain_default = getattr(settings, "get_bool", lambda *a, **k: True)(
        "DW_INCLUDE_EXPLAIN", True
    )
    include_explain = (
        bool(include_explain_req)
        if include_explain_req is not None
        else bool(include_explain_default)
    )

    explain_text = None
    if intent.explain_on and include_explain:
        explain_text = build_explain(intent)

    result_meta = {
        "binds": binds,
        "wants_all_columns": intent.wants_all_columns,
        "rowcount": len(rows),
        "attempt_no": 1,
        "strategy": "deterministic",
        "fts": fts_meta,
    }

    result: Dict[str, Any] = {
        "sql": sql,
        "rows": rows,
        "columns": cols,
        "csv_path": exec_meta.get("csv_path"),
        "meta": result_meta,
        "debug": debug,
        "ok": True,
    }
    if explain_text:
        result["explain"] = explain_text

    with mem_engine.begin() as cx:
        cx.execute(
            text(
                """
            INSERT INTO mem_runs(namespace, input_query, status, context_pack, sql_final, rows_returned, created_at, completed_at)
            VALUES(:ns, :q, 'complete', :ctx, :sql, :rows, NOW(), NOW())
        """
            ),
            {
                "ns": namespace,
                "q": question,
                "ctx": json.dumps(
                    {"inquiry_id": inquiry_id, "attempt_no": 1, "strategy": "deterministic"}
                ),
                "sql": result["sql"],
                "rows": len(result.get("rows", [])),
            },
        )
        cx.execute(
            text(
                """
            UPDATE mem_inquiries SET status = 'answered', updated_at = NOW() WHERE id = :iid
        """
            ),
            {"iid": inquiry_id},
        )

    payload: Dict[str, Any] = {"ok": True, "inquiry_id": inquiry_id, **result}
    if "explain" in payload:
        payload["llm_explain"] = payload["explain"]
    payload["explain"] = _build_user_explain(intent, binds, sql)
    return jsonify(payload)


def create_dw_blueprint(*args, **kwargs):
    return dw_bp
