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
    try:
        append_admin_note(mem, inquiry_id, by=answered_by, text_note=admin_reply)
    except Exception as e:
        return (
            jsonify({"error": f"append_failed: {e.__class__.__name__}: {e}"}),
            400,
        )

    pipeline = current_app.config["PIPELINE"]
    out = pipeline.process_admin_reply(inquiry_id)
    return jsonify(out), 200


@admin_bp.post("/inquiries/reply")
def admin_reply_body():
    if not _check_admin_key():
        return jsonify({"error": "unauthorized"}), 401

    data = request.get_json(force=True) or {}
    inquiry_id = int(data.get("inquiry_id") or 0)
    answered_by = (data.get("auth_email") or "").strip()
    admin_reply = (data.get("answer") or "").strip()
    if not (inquiry_id and answered_by and admin_reply):
        return (
            jsonify({"error": "inquiry_id, auth_email, answer are required"}),
            400,
        )

    mem = current_app.config["MEM_ENGINE"]
    try:
        append_admin_note(mem, inquiry_id, by=answered_by, text_note=admin_reply)
    except Exception as e:
        return (
            jsonify({"error": f"append_failed: {e.__class__.__name__}: {e}"}),
            400,
        )

    pipeline = current_app.config["PIPELINE"]
    out = pipeline.process_admin_reply(inquiry_id)
    return jsonify(out), 200

