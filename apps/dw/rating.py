from __future__ import annotations

from flask import Blueprint, jsonify, request

from apps.dw.logger import log
from apps.dw.rate_pipeline import run_rate

rate_bp = Blueprint("rate_bp", __name__)


@rate_bp.route("/rate", methods=["POST"])
def rate():
    payload = request.get_json() or {}
    inquiry_id = payload.get("inquiry_id")
    comment = (payload.get("comment") or "").strip()

    log.info(
        {
            "event": "rate.receive",
            "inquiry_id": inquiry_id,
            "payload": {
                "inquiry_id": inquiry_id,
                "rating": payload.get("rating"),
            },
            "comment_len": len(comment),
        }
    )

    try:
        result = run_rate(comment, table="Contract")
        log.info(
            {
                "event": "rate.sql.exec",
                "inquiry_id": inquiry_id,
                "sql": result["sql"],
                "binds": result["binds"],
            }
        )

        response = {
            "ok": True,
            "retry": False,
            "inquiry_id": inquiry_id,
            "sql": result["sql"],
            "rows": result["rows"],
            "debug": result["debug"],
            "binds": result["binds"],
        }
        log.info({"event": "rate.response", "inquiry_id": inquiry_id, "retry": False})
        return jsonify(response), 200
    except Exception as exc:  # pragma: no cover - defensive
        log.exception("rate.failed")
        return jsonify({"ok": False, "error": str(exc), "inquiry_id": inquiry_id}), 500
