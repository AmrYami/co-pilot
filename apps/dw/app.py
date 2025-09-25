from __future__ import annotations

import json
import os
from typing import Any, Dict

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

from .intent import parse_intent
from .sqlgen import build_sql
from .fts import build_fts_clause
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


@dw_bp.post("/answer")
def answer():
    app = current_app
    data = request.get_json(force=True) or {}
    question = (data.get("question") or "").strip()
    prefixes = data.get("prefixes") or []
    auth_email = (data.get("auth_email") or "").strip()
    namespace = data.get("namespace") or NAMESPACE
    fts_requested = data.get("full_text_search")

    if not question:
        return jsonify({"ok": False, "error": "question required"}), 400

    mem_engine = app.config["MEM_ENGINE"]

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

    intent = parse_intent(question, full_text_search=bool(fts_requested))
    sql, binds = build_sql(intent)

    fts_meta: Dict[str, Any] = {
        "enabled": intent.full_text_search,
        "tokens": None,
        "columns": None,
        "binds": None,
        "error": None,
    }
    if intent.full_text_search:
        try:
            where_fts, binds_fts, toks, cols_used = build_fts_clause(question, columns=None)
            intent.fts_tokens = toks
            fts_meta["tokens"] = toks
            fts_meta.setdefault("columns", cols_used)
            if where_fts:
                if " WHERE " in sql:
                    sql = sql.replace(" WHERE ", f" WHERE {where_fts} AND ", 1)
                else:
                    sql = sql + "\nWHERE " + where_fts
                binds.update(binds_fts)
                fts_meta.update({"tokens": toks, "binds": list(binds_fts.keys())})
        except Exception as exc:  # pragma: no cover - defensive guard
            fts_meta["error"] = str(exc)

    rows, cols, exec_meta = _execute_oracle(sql, binds)

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
