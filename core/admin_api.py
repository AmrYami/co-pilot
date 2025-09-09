from __future__ import annotations
from typing import Any, Dict
from flask import Blueprint, current_app, jsonify, request
from sqlalchemy.engine import Engine

from core.inquiries import append_admin_note, get_inquiry, set_inquiry_status

admin_bp = Blueprint("admin", __name__)


def _check_admin_key() -> bool:
    # Accept either plaintext SETTINGS_ADMIN_KEY or a yet-to-be-added hash check
    key_hdr = request.headers.get("X-Admin-Key", "")
    s = current_app.config["SETTINGS"]
    expect = (s.get("SETTINGS_ADMIN_KEY") or "").strip()
    return bool(expect and key_hdr and key_hdr == expect)


@admin_bp.post("/admin/inquiries/<int:inquiry_id>/reply")
def admin_reply(inquiry_id: int):
    if not _check_admin_key():
        return jsonify({"error": "unauthorized"}), 401

    data: Dict[str, Any] = request.get_json(force=True) or {}
    admin_reply = (data.get("admin_reply") or "").strip()
    answered_by = (data.get("answered_by") or "admin@example.com").strip()

    if not admin_reply:
        return jsonify({"error": "admin_reply required"}), 400

    pipeline = current_app.config["PIPELINE"]
    mem: Engine = current_app.config["MEM_ENGINE"]

    inq = get_inquiry(mem, inquiry_id)
    if not inq:
        return jsonify({"error": "inquiry_not_found"}), 404

    # Persist the note
    append_admin_note(mem, inquiry_id, by=answered_by, text_note=admin_reply)

    # Try to derive/plan again with ALL notes so far
    try:
        result = pipeline.retry_from_admin(
            inquiry_id=inquiry_id,
            source="fa",
            prefixes=inq.get("prefixes") or [],
            question=inq.get("question") or "",
            answered_by=answered_by
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    # Update status
    if result.get("status") == "ok":
        set_inquiry_status(mem, inquiry_id, status="answered", answered_by=answered_by)
    else:
        set_inquiry_status(mem, inquiry_id, status="needs_clarification", answered_by=answered_by)

    return jsonify({"inquiry_id": inquiry_id, **result})

