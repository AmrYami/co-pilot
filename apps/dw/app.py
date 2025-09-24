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

from .attempts import run_attempt
from .rating import rate_bp

NAMESPACE = os.getenv("DW_NAMESPACE", "dw::common")


dw_bp = Blueprint("dw", __name__)
dw_bp.register_blueprint(rate_bp, url_prefix="")


@dw_bp.post("/answer")
def answer():
    app = current_app
    data = request.get_json(force=True) or {}
    question = (data.get("question") or "").strip()
    prefixes = data.get("prefixes") or []
    auth_email = (data.get("auth_email") or "").strip()
    namespace = data.get("namespace") or NAMESPACE

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

    result = run_attempt(question, namespace, attempt_no=1, strategy="deterministic")

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
