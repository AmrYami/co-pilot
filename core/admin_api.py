from __future__ import annotations
from flask import Blueprint, current_app, request, jsonify

from core.inquiries import append_admin_note

admin_bp = Blueprint("admin", __name__, url_prefix="/admin")


def _check_admin_key() -> bool:
    # Accept either plaintext SETTINGS_ADMIN_KEY or a yet-to-be-added hash check
    key_hdr = request.headers.get("X-Admin-Key", "")
    s = current_app.config["SETTINGS"]
    expect = (s.get("SETTINGS_ADMIN_KEY") or "").strip()
    return bool(expect and key_hdr and key_hdr == expect)


@admin_bp.post("/inquiries/<int:inquiry_id>/reply")
def admin_reply(inquiry_id: int):
    if not _check_admin_key():
        return jsonify({"error": "unauthorized"}), 401

    data = request.get_json(force=True) or {}
    answered_by = (data.get("answered_by") or data.get("by") or "").strip() or "admin"
    admin_reply = (data.get("admin_reply") or data.get("reply") or data.get("answer") or "").strip()

    if not admin_reply:
        return jsonify({"error": "admin_reply is required"}), 400

    mem = current_app.config["MEM_ENGINE"]
    pipeline = current_app.config["PIPELINE"]

    note_info = append_admin_note(mem, inquiry_id, by=answered_by, text_note=admin_reply)

    try:
        out = pipeline.replan_from_admin_notes(inquiry_id, answered_by=answered_by)
        return jsonify(out), 200
    except Exception as e:
        current_app.logger.exception("replan_from_admin_notes failed")
        return jsonify({
            "inquiry_id": inquiry_id,
            "status": "needs_clarification",
            "message": "Note saved; add one more hint or confirm tables.",
        }), 200

