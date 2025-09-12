from __future__ import annotations
from flask import Blueprint, request, jsonify, current_app
from werkzeug.exceptions import BadRequest
from sqlalchemy import text, bindparam
from sqlalchemy.dialects.postgresql import JSONB
from core.inquiries import append_admin_note, fetch_inquiry

admin_bp = Blueprint("admin_bp", __name__, url_prefix="/admin")


def _conn():
    # Use the same engine the Pipeline made available
    mem_engine = current_app.config.get("MEM_ENGINE")
    if mem_engine is None:
        raise RuntimeError("MEM_ENGINE not configured on app")
    return mem_engine.connect()


@admin_bp.post("/settings/bulk")
def settings_bulk():
    mem = current_app.config["MEM_ENGINE"]
    payload = request.get_json(force=True) or {}
    ns = payload.get("namespace") or "fa::common"
    upd = payload.get("updated_by") or "api"
    items = payload.get("settings") or []

    upsert = (
        text(
            """
            INSERT INTO mem_settings(
                namespace, key, value, value_type, scope, scope_id,
                category, description, overridable, updated_by,
                created_at, updated_at, is_secret
            )
            VALUES (
                :ns, :key, :val, :vtype, :scope, :scope_id,
                :cat, :desc, COALESCE(:ovr, true), :upd,
                NOW(), NOW(), COALESCE(:sec, false)
            )
            ON CONFLICT (namespace, key, scope, COALESCE(scope_id, ''))
            DO UPDATE SET
                value      = EXCLUDED.value,
                value_type = EXCLUDED.value_type,
                updated_by = EXCLUDED.updated_by,
                updated_at = NOW(),
                is_secret  = EXCLUDED.is_secret
            """
        ).bindparams(bindparam("val", type_=JSONB))
    )

    def infer_type(v, explicit):
        if explicit:
            return explicit
        if isinstance(v, bool):
            return "bool"
        if isinstance(v, int):
            return "int"
        if isinstance(v, (dict, list)):
            return "json"
        return "string"

    updated = 0
    with mem.begin() as conn:
        for s in items:
            key = s["key"]
            val = s.get("value")
            vtype = infer_type(val, s.get("value_type"))
            scope = s.get("scope") or "namespace"

            conn.execute(
                upsert,
                {
                    "ns": ns,
                    "key": key,
                    "val": val,
                    "vtype": vtype,
                    "scope": scope,
                    "scope_id": s.get("scope_id"),
                    "cat": s.get("category"),
                    "desc": s.get("description"),
                    "ovr": s.get("overridable"),
                    "upd": upd,
                    "sec": bool(s.get("is_secret")),
                },
            )
            updated += 1

    return {"ok": True, "updated": updated}, 200


@admin_bp.get("/settings/get")
def settings_get():
    """Quick fetch: /admin/settings/get?namespace=fa::common&key=RESEARCH_MODE"""
    ns = request.args.get("namespace", "default")
    key = request.args.get("key")
    where = "WHERE namespace = :ns"
    params = {"ns": ns}
    if key:
        where += " AND key = :k"
        params["k"] = key
    sql = text(f"SELECT id, namespace, key, value, value_type, scope, scope_id, updated_at FROM mem_settings {where} ORDER BY key;")
    with _conn() as c:
        rows = [dict(r._mapping) for r in c.execute(sql, params)]
    return jsonify({"ok": True, "items": rows})


# ----- Admin reply (append note safely & drive the pipeline) -----

@admin_bp.get("/inquiries/<int:inq_id>")
def get_inquiry(inq_id: int):
    mem = current_app.config["MEM_ENGINE"]
    row = fetch_inquiry(mem, inq_id)
    if not row:
        return jsonify({"error": "not_found"}), 404
    return jsonify(row)


@admin_bp.post("/inquiries/<int:inq_id>/reply")
def admin_reply(inq_id: int):
    mem = current_app.config["MEM_ENGINE"]
    data = request.get_json(force=True) or {}

    answered_by = data.get("answered_by") or data.get("by") or "admin"
    admin_reply = data.get("admin_reply") or data.get("reply")

    if not admin_reply:
        return jsonify({"ok": False, "error": "admin_reply is required"}), 400

    try:
        rounds = append_admin_note(mem, inq_id, by=answered_by, text_note=admin_reply)
    except Exception as e:
        return jsonify({"ok": False, "error": f"append_failed: {e}"}), 500

    # Optional auto process if client asks
    if str(data.get("process", "0")).lower() in {"1", "true", "yes", "y"}:
        pipeline = current_app.config["PIPELINE"]
        try:
            # inline=True → don’t send emails or escalate; just return JSON
            out = pipeline.apply_admin_and_retry(inq_id, inline=True)
            return jsonify({"ok": True, "inquiry_id": inq_id, **out})
        except Exception as e:
            return jsonify({"ok": False, "error": f"process_failed: {e}", "inquiry_id": inq_id}), 500

    return jsonify({"ok": True, "inquiry_id": inq_id, "clarification_rounds": rounds}), 200


@admin_bp.post("/inquiries/<int:inq_id>/process")
def admin_process(inq_id: int):
    """Re-run an inquiry using stored question + admin notes."""
    pipeline = current_app.config.get("PIPELINE")
    if pipeline is None:
        raise BadRequest("Pipeline not available")
    try:
        out = pipeline.apply_admin_and_retry(inq_id, inline=True)
        # Shape is either {"status":"answered", ...} or {"status":"needs_clarification", ...}
        return jsonify({"ok": True, "inquiry_id": inq_id, **out})
    except Exception as e:
        return jsonify({"ok": False, "inquiry_id": inq_id, "error": str(e)}), 500
