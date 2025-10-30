from __future__ import annotations

from flask import Blueprint, jsonify, request

from apps.dw.logger import log
from apps.dw.order_utils import normalize_order_hint
from apps.dw.rate_pipeline import run_rate

rate_bp = Blueprint("rate_bp", __name__)


@rate_bp.route("/rate", methods=["POST"])
def rate():
    payload = request.get_json() or {}
    inquiry_id = payload.get("inquiry_id")
    comment = (payload.get("comment") or "").strip()
    rating = payload.get("rating")

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
        result = run_rate(inquiry_id=inquiry_id, rating=rating, comment=comment)
        # Log full response without rows for observability
        try:
            if isinstance(result, dict):
                resp_copy = dict(result)
                rows_field = resp_copy.get("rows")
                try:
                    rows_count = len(rows_field) if isinstance(rows_field, list) else int(rows_field or 0)
                except Exception:
                    rows_count = 0
                if "rows" in resp_copy:
                    resp_copy["rows"] = f"omitted({rows_count})"
                log.info({"event": "rate.response.full", "inquiry_id": inquiry_id, "response": resp_copy})
        except Exception:
            pass
        log.info({"event": "rate.response", "inquiry_id": inquiry_id, "retry": False})

        final_sql_str = result.get("sql") if isinstance(result, dict) else None
        binds_dict = {}
        intent_dict = {}
        if isinstance(result, dict):
            binds = result.get("binds")
            if isinstance(binds, dict):
                binds_dict = dict(binds)
            debug = result.get("debug") or {}
            if isinstance(debug, dict):
                intent = debug.get("intent")
                if isinstance(intent, dict):
                    normalized_intent = dict(intent)
                    sort_by, sort_desc = normalize_order_hint(
                        normalized_intent.get("sort_by"), normalized_intent.get("sort_desc")
                    )
                    if sort_by:
                        normalized_intent["sort_by"] = sort_by
                    else:
                        normalized_intent.pop("sort_by", None)
                    if sort_desc is None:
                        normalized_intent.pop("sort_desc", None)
                    else:
                        normalized_intent["sort_desc"] = sort_desc
                    debug["intent"] = normalized_intent
                    intent_dict = normalized_intent

        if inquiry_id is not None:
            from apps.dw.feedback_repo import persist_feedback as _persist_dw_feedback

            try:
                log.info({"event": "rate.persist.attempt", "inquiry_id": inquiry_id})
                _persist_dw_feedback(
                    inquiry_id=inquiry_id,
                    auth_email=payload.get("auth_email"),
                    # Be tolerant to missing/None ratings
                    rating=int(rating or 0),
                    comment=comment or "",
                    intent=intent_dict,
                    resolved_sql=final_sql_str or "",
                    binds=binds_dict,
                )
                log.info({"event": "rate.persist.ok", "inquiry_id": inquiry_id})
            except Exception as exc:  # pragma: no cover - defensive logging
                log.exception(
                    {
                        "event": "rate.persist.err",
                        "inquiry_id": inquiry_id,
                        "err": str(exc),
                    }
                )

        return jsonify(result), 200
    except Exception as exc:  # pragma: no cover - defensive
        log.exception("rate.failed")
        return jsonify({"ok": False, "error": str(exc), "inquiry_id": inquiry_id}), 500
