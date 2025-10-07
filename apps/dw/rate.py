from flask import Blueprint, request, jsonify

from apps.dw.sql_builder import build_rate_sql
from apps.dw.settings import get_setting
from apps.dw.learning import record_feedback, to_patch_from_comment

rate_bp = Blueprint("rate", __name__)


@rate_bp.route("/dw/rate", methods=["POST"])
def rate():
    payload = request.get_json(force=True, silent=True) or {}
    inquiry_id = payload.get("inquiry_id")
    rating = payload.get("rating")
    comment = (payload.get("comment") or "").strip()
    record_feedback(inquiry_id=inquiry_id, rating=rating, comment=comment)

    patch = None
    if rating is not None and int(rating) <= 2 and comment:
        patch = to_patch_from_comment(comment)

    resp = {"ok": True, "inquiry_id": inquiry_id, "debug": {}}

    if patch:
        intent = {
            "eq_filters": patch.get("eq_filters") or [],
            "fts": {
                "enabled": bool(patch.get("fts_tokens")),
                "operator": patch.get("fts_operator") or "OR",
                "tokens": [[t] for t in (patch.get("fts_tokens") or [])],
                "columns": (get_setting("DW_FTS_COLUMNS", scope="namespace") or {}).get("Contract", []),
            },
            "group_by": patch.get("group_by"),
            "sort_by": patch.get("sort_by"),
            "sort_desc": patch.get("sort_desc"),
            "top_n": patch.get("top_n"),
            "gross": patch.get("gross"),
        }
    else:
        intent = {
            "eq_filters": [],
            "fts": {"enabled": False},
            "group_by": None,
            "sort_by": "REQUEST_DATE",
            "sort_desc": True,
            "top_n": None,
            "gross": None,
        }

    enum_syn = (get_setting("DW_ENUM_SYNONYMS", scope="namespace") or {}).get("Contract.REQUEST_TYPE", {})
    sql, binds = build_rate_sql(intent, enum_syn=enum_syn)
    resp.update(
        {
            "retry": True,
            "sql": sql,
            "debug": {
                "intent": intent,
                "rate_hints": patch or {},
                "validation": {
                    "ok": True,
                    "binds": list(binds.keys()),
                    "bind_names": list(binds.keys()),
                    "errors": [],
                },
            },
            "meta": {
                "attempt_no": 2,
                "binds": binds,
                "clarifier_intent": intent,
                "strategy": "det_overlaps_gross",
                "wants_all_columns": True,
            },
        }
    )
    return jsonify(resp), 200
