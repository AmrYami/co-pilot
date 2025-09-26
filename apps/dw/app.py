import json
import os
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
    from sqlalchemy import text
except Exception:  # pragma: no cover - lightweight fallback used in tests
    def text(sql: str):  # type: ignore
        return sql

from core.settings import Settings

from apps.dw.explain import build_explanation, explain_interpretation
from apps.dw.fts import build_predicate, load_columns, tokenize
from .intent import parse_intent
from .planner_det import build_sql
from .rating import rate_bp

NAMESPACE = os.getenv("DW_NAMESPACE", "dw::common")


dw_bp = Blueprint("dw", __name__)
dw_bp.register_blueprint(rate_bp, url_prefix="")


def _execute_oracle(sql: str, binds: Dict[str, Any]):
    app = current_app
    rows: List[List[Any]] = []
    cols: List[str] = []
    meta: Dict[str, Any] = {
        "validation": {"ok": True, "errors": [], "binds": list(binds.keys())},
        "csv_path": None,
    }
    engine = app.config.get("DW_ENGINE") if app else None  # type: ignore[assignment]
    if engine is None:
        return rows, cols, meta
    with engine.connect() as cx:  # type: ignore[union-attr]
        rs = cx.execute(text(sql), binds)
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


@dw_bp.post("/answer")
def answer():
    app = current_app
    data = request.get_json(force=True) or {}
    question = (data.get("question") or "").strip()
    prefixes = data.get("prefixes") or []
    auth_email = (data.get("auth_email") or "").strip()
    namespace = data.get("namespace") or NAMESPACE
    fts_requested = data.get("full_text_search")
    full_text_search = bool(fts_requested)

    if not question:
        return jsonify({"ok": False, "error": "question required"}), 400

    mem_engine = app.config["MEM_ENGINE"]
    settings = Settings(namespace=namespace)
    settings.mem_engine = lambda: mem_engine

    with mem_engine.begin() as cx:
        row = cx.execute(
            text(
                """
            INSERT INTO mem_inquiries(namespace, prefixes, question, auth_email, status, created_at, updated_at)
            VALUES(:ns, :pfx, :q, :mail, 'open', NOW(), NOW())
            RETURNING id
        """
            ),
            {"ns": namespace, "pfx": json.dumps(prefixes), "q": question, "mail": auth_email},
        ).fetchone()
    inquiry_id = int(row[0]) if row else None
    app.logger.info(
        "[dw] inquiry_start: %s",
        json.dumps({"id": inquiry_id, "q": question, "email": auth_email, "ns": namespace, "prefixes": prefixes}),
    )

    contract_table = settings.get("DW_CONTRACT_TABLE", "Contract")
    contract_table_name = str(contract_table)

    intent = parse_intent(question, settings)
    intent["full_text_search"] = full_text_search

    # Full-text search setup
    fts_meta = {"enabled": False, "tokens": None, "columns": None, "binds": None, "error": None}
    fts_where = ""
    fts_bind = {}
    if full_text_search:
        try:
            tokens = tokenize(question)
            cols = load_columns(settings, table=contract_table_name)
            fts_meta.update({"enabled": True, "tokens": tokens, "columns": cols})
            intent["fts_tokens"] = tokens
            if cols and tokens:
                fts_where, fts_bind = build_predicate(cols, tokens)
        except Exception as exc:  # pragma: no cover - defensive log only
            fts_meta["error"] = str(exc)

    sql, binds, planner_explain = build_sql(intent, settings)

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

    ft_bind_keys = [k for k in binds.keys() if k.startswith("fts")]
    fts_meta.update({"binds": ft_bind_keys or None})

    explain = explain_interpretation(intent, binds, table=contract_table_name)

    debug = {
        "intent": _intent_debug(intent),
        "prompt": "",
        "validation": exec_meta.get("validation", {}),
        "fts": fts_meta,
    }

    result: Dict[str, Any] = {
        "sql": sql,
        "rows": rows,
        "columns": cols,
        "csv_path": exec_meta.get("csv_path"),
        "meta": {
            "binds": binds,
            "wants_all_columns": intent.get("wants_all_columns"),
            "rowcount": len(rows),
            "attempt_no": 1,
            "strategy": "deterministic",
            "fts": fts_meta,
            "builder": {"planner": "deterministic", "explain": planner_explain},
        },
        "debug": debug,
        "ok": True,
        "explain": explain or planner_explain,
    }

    include_explain_req = data.get("include_explain")
    include_explain_default = settings.get_bool("DW_INCLUDE_EXPLAIN", True)
    include_explain = include_explain_req if include_explain_req is not None else include_explain_default

    if include_explain and not result.get("explain"):
        meta_obj = result.get("meta")
        if not isinstance(meta_obj, dict):
            meta_obj = {}
        columns_selected = meta_obj.get("projection_columns") or cols
        try:
            result["explain"] = build_explanation(
                intent=_intent_debug(intent),
                binds=meta_obj.get("binds", {}),
                fts_meta=meta_obj.get("fts", {}),
                table=str(contract_table_name),
                cols_selected=columns_selected or [],
                strategy=meta_obj.get("strategy", ""),
                default_date_basis=settings.get("DW_DATE_COLUMN", "REQUEST_DATE"),
            )
        except Exception as _e:  # pragma: no cover - defensive logging only
            if isinstance(result.get("debug"), dict):
                result["debug"]["explain_error"] = str(_e)

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
                "ctx": json.dumps({"inquiry_id": inquiry_id, "attempt_no": 1, "strategy": "deterministic"}),
                "sql": result["sql"],
                "rows": len(result["rows"]),
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
    return jsonify(payload)


def create_dw_blueprint(*args, **kwargs):
    return dw_bp
