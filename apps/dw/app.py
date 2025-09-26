from __future__ import annotations

import json
import os
from typing import Any, Dict, Tuple

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

from apps.dw.explain import build_explanation

from .config import get_dw_fts_columns
from .intent import NLIntent, parse_intent
from .sql_builder import build_sql
from .rating import rate_bp

NAMESPACE = os.getenv("DW_NAMESPACE", "dw::common")


dw_bp = Blueprint("dw", __name__)
dw_bp.register_blueprint(rate_bp, url_prefix="")


def _execute_oracle(sql: str, binds: Dict[str, Any]):
    app = current_app
    rows: list[list[Any]] = []
    cols: list[str] = []
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


def _quote_table(name: str) -> str:
    n = (name or "Contract").strip()
    if n.startswith("\"") and n.endswith("\""):
        return n
    return f'"{n}"'


def _intent_debug(intent: NLIntent) -> Dict[str, Any]:
    return {
        "agg": intent.agg,
        "date_column": intent.date_column,
        "expire": intent.expire,
        "explicit_dates": intent.explicit_dates,
        "full_text_search": intent.full_text_search,
        "fts_tokens": intent.fts_tokens,
        "group_by": intent.group_by,
        "has_time_window": intent.has_time_window,
        "measure_sql": intent.measure_sql,
        "sort_by": intent.sort_by,
        "sort_desc": intent.sort_desc,
        "top_n": intent.top_n,
        "user_requested_top_n": intent.user_requested_top_n,
        "wants_all_columns": intent.wants_all_columns,
    }


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
    contract_table_sql = _quote_table(str(contract_table))

    prefer_overlap_default = True
    require_window_flag = settings.get("DW_REQUIRE_WINDOW_FOR_EXPIRE", scope="namespace", default=1)
    try:
        require_window_for_expire = bool(int(str(require_window_flag)))
    except Exception:
        require_window_for_expire = bool(require_window_flag)

    overlap_flag = settings.get("DW_OVERLAP_STRICT", scope="namespace", default=1)
    try:
        overlap_strict = bool(int(str(overlap_flag)))
    except Exception:
        overlap_strict = bool(overlap_flag)

    intent = parse_intent(
        question,
        prefer_overlap_default=prefer_overlap_default,
        require_window_for_expire=require_window_for_expire,
        full_text_search=full_text_search,
    )

    fts_columns = get_dw_fts_columns(settings, contract_table)
    sql, binds = build_sql(
        intent,
        table=contract_table_sql,
        overlap_strict=overlap_strict,
        fts_columns=fts_columns if full_text_search else None,
    )

    rows, cols, exec_meta = _execute_oracle(sql, binds)

    ft_bind_keys = [k for k in binds.keys() if k.startswith("ft_")]
    fts_meta: Dict[str, Any] = {
        "enabled": full_text_search and bool(intent.fts_tokens),
        "tokens": intent.fts_tokens,
        "columns": fts_columns if full_text_search else None,
        "binds": ft_bind_keys or None,
        "error": None,
    }

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
            "wants_all_columns": intent.wants_all_columns,
            "rowcount": len(rows),
            "attempt_no": 1,
            "strategy": "deterministic",
            "fts": fts_meta,
        },
        "debug": debug,
        "ok": True,
    }

    # ---------- Optional end-user explanation ----------
    include_explain_req = data.get("include_explain")
    include_explain_default = settings.get_bool("DW_INCLUDE_EXPLAIN", True)
    include_explain = include_explain_req if include_explain_req is not None else include_explain_default

    if include_explain:
        meta_obj = result.get("meta")
        if not isinstance(meta_obj, dict):
            meta_obj = {}
        columns_selected = meta_obj.get("projection_columns")
        if not columns_selected:
            columns_selected = cols
        try:
            result["explain"] = build_explanation(
                intent=_intent_debug(intent),
                binds=meta_obj.get("binds", {}),
                fts_meta=meta_obj.get("fts", {}),
                table=str(contract_table),
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
