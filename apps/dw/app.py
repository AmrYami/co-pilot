from __future__ import annotations

import json
import os
import re
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

from .intent import parse_intent
from .sqlgen import build_sql
from .rating import rate_bp
from .fts import build_oracle_fts_predicate, tokenize as fts_tokenize

NAMESPACE = os.getenv("DW_NAMESPACE", "dw::common")


dw_bp = Blueprint("dw", __name__)
dw_bp.register_blueprint(rate_bp, url_prefix="")


def _apply_fts_if_needed(
    sql: str,
    binds: Dict[str, Any],
    question: str,
    settings: Settings,
    table_name: str,
) -> Tuple[str, Dict[str, Any], Dict[str, Any]]:
    debug: Dict[str, Any] = {
        "enabled": False,
        "tokens": None,
        "columns": None,
        "binds": None,
        "error": None,
    }
    try:
        columns = settings.get_fts_columns(table_name)
        debug["columns"] = columns
        if not columns:
            return sql, binds, debug

        tokens = fts_tokenize(question or "")
        debug["tokens"] = tokens
        if not tokens:
            return sql, binds, debug

        tokens_mode = (settings.get("DW_FTS_TOKENS_MODE", "all") or "all").lower()
        fragment, fts_binds = build_oracle_fts_predicate(
            tokens,
            columns,
            bind_prefix="fts",
            tokens_mode=tokens_mode,
        )
        if not fragment:
            return sql, binds, debug

        sql_upper = sql.upper()
        order_idx = sql_upper.find(" ORDER BY ")
        head = sql if order_idx < 0 else sql[:order_idx]
        tail = "" if order_idx < 0 else sql[order_idx:]

        if re.search(r"\bWHERE\b", head, flags=re.I):
            head = head.rstrip() + f"\n  AND {fragment}\n"
        else:
            head = head.rstrip() + f"\nWHERE {fragment}\n"

        merged_binds = dict(binds)
        merged_binds.update(fts_binds)
        debug["binds"] = list(merged_binds.keys())
        debug["enabled"] = True
        return head + tail, merged_binds, debug
    except Exception as exc:  # pragma: no cover - defensive guard
        debug["error"] = str(exc)
        return sql, binds, debug


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

    intent = parse_intent(question, full_text_search=full_text_search)
    final_sql, binds = build_sql(intent)

    fts_meta: Dict[str, Any] = {
        "enabled": False,
        "tokens": None,
        "columns": None,
        "binds": None,
        "error": None,
    }
    if full_text_search:
        contract_table = settings.get("DW_CONTRACT_TABLE", "Contract")
        final_sql, binds, fts_meta = _apply_fts_if_needed(
            final_sql,
            binds,
            question,
            settings,
            contract_table,
        )
        if fts_meta.get("tokens"):
            intent.fts_tokens = fts_meta["tokens"]

    rows, cols, exec_meta = _execute_oracle(final_sql, binds)

    result: Dict[str, Any] = {
        "sql": final_sql,
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
        "debug": {
            "intent": intent.__dict__,
            "prompt": "",
            "validation": exec_meta.get("validation", {}),
            "fts": fts_meta,
        },
        "ok": True,
    }

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
