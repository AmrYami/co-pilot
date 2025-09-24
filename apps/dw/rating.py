from __future__ import annotations

import json
from typing import Dict

try:  # pragma: no cover - allow unit tests without Flask dependency
    from flask import Blueprint, current_app, jsonify, request
except Exception:  # pragma: no cover - lightweight fallback used in tests
    current_app = None  # type: ignore[assignment]

    class _StubBlueprint:
        def __init__(self, *args, **kwargs):
            pass

        def post(self, *args, **kwargs):
            def decorator(fn):
                return fn

            return decorator

    def _jsonify(*args, **kwargs):
        return {}

    class _StubRequest:
        args: Dict[str, str] = {}

        def get_json(self, force: bool = False):
            return {}

    Blueprint = _StubBlueprint  # type: ignore[assignment]
    jsonify = _jsonify  # type: ignore[assignment]
    request = _StubRequest()  # type: ignore[assignment]
try:  # pragma: no cover - optional dependency in tests
    from sqlalchemy import text
except Exception:  # pragma: no cover
    def text(sql: str):  # type: ignore
        return sql

from .attempts import run_attempt
from .utils import env_flag, env_int

rate_bp = Blueprint("dw_rate", __name__)


@rate_bp.post("/rate")
def rate():
    app = current_app
    engine = app.config["MEM_ENGINE"]
    data = request.get_json(force=True) or {}
    inquiry_id = int(data.get("inquiry_id") or 0)
    rating = int(data.get("rating") or 0)
    feedback = (data.get("feedback") or "").strip() or None
    if not inquiry_id or rating < 1 or rating > 5:
        return jsonify({"ok": False, "error": "invalid payload"}), 400

    with engine.begin() as cx:
        cx.execute(
            text(
                """
            UPDATE mem_inquiries
               SET rating = :r,
                   feedback_comment = COALESCE(:fb, feedback_comment),
                   satisfied = CASE WHEN :r >= 4 THEN TRUE ELSE NULL END,
                   updated_at = NOW()
             WHERE id = :iid
        """
            ),
            {"r": rating, "fb": feedback, "iid": inquiry_id},
        )

    if rating < 3 and env_int("DW_MAX_RERUNS", 1) > 0:
        alt_strategy = (
            request.args.get("strategy")
            or (env_flag("DW_ACCURACY_FIRST", True) and "det_overlaps_gross")
            or "deterministic"
        )
        with engine.connect() as cx:
            row = cx.execute(
                text(
                    """
                SELECT namespace, question
                  FROM mem_inquiries
                 WHERE id = :iid
            """
                ),
                {"iid": inquiry_id},
            ).fetchone()
        if row:
            ns, q = row[0], row[1]
            alt = run_attempt(q, ns, attempt_no=2, strategy=alt_strategy)
            with engine.begin() as cx:
                cx.execute(
                    text(
                        """
                    INSERT INTO mem_runs(namespace, input_query, status, context_pack, created_at)
                    VALUES(:ns, :q, 'complete', :ctx, NOW())
                """
                    ),
                    {
                        "ns": ns,
                        "q": q,
                        "ctx": json.dumps(
                            {
                                "inquiry_id": inquiry_id,
                                "attempt_no": 2,
                                "strategy": alt_strategy,
                            }
                        ),
                    },
                )
            return jsonify({"ok": True, "retry": True, "inquiry_id": inquiry_id, **alt})

        return jsonify({"ok": True, "retry": False, "inquiry_id": inquiry_id})

    if rating < 3 and env_flag("DW_ESCALATE_ON_LOW_RATING", True):
        with engine.begin() as cx:
            cx.execute(
                text(
                    """
                INSERT INTO mem_alerts(namespace, event_type, recipient, payload, status, created_at)
                VALUES(:ns, 'low_rating', :rcpt, :payload, 'queued', NOW())
            """
                ),
                {
                    "ns": "dw::common",
                    "rcpt": "admin@example.com",
                    "payload": json.dumps(
                        {"inquiry_id": inquiry_id, "rating": rating, "feedback": feedback}
                    ),
                },
            )

    return jsonify({"ok": True, "retry": False, "inquiry_id": inquiry_id})
