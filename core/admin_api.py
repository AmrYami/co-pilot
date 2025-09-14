from __future__ import annotations
from flask import Blueprint, request, jsonify, current_app
from werkzeug.exceptions import BadRequest
from sqlalchemy import text
from core.inquiries import append_admin_note, fetch_inquiry
from core.settings import Settings
from core.memdb import MemoryDB
import json

admin_bp = Blueprint("admin", __name__, url_prefix="/admin")


def _conn():
    # Use the same engine the Pipeline made available
    mem_engine = current_app.config.get("MEM_ENGINE")
    if mem_engine is None:
        raise RuntimeError("MEM_ENGINE not configured on app")
    return mem_engine.connect()


@admin_bp.post("/settings/bulk")
def settings_bulk():
    """Idempotent upsert of settings. Values are stored as JSONB."""
    payload = request.get_json(force=True) or {}
    ns = payload.get("namespace") or "fa::common"
    updated_by = payload.get("updated_by") or "api"
    items = payload.get("settings") or []

    mem = MemoryDB(Settings())
    written = 0

    upsert_sql = text(
        """
        INSERT INTO mem_settings(
            namespace, key, value, value_type, scope, scope_id,
            category, description, overridable, updated_by, created_at, updated_at, is_secret
        )
        VALUES (
            :ns, :key, CAST(:val AS jsonb), :vtype, :scope, :scope_id,
            :cat, :desc, COALESCE(:ovr, true), :upd_by, NOW(), NOW(), :is_secret
        )
        ON CONFLICT ON CONSTRAINT ux_mem_settings_ns_key_scope_coalesced
        DO UPDATE SET
            value      = EXCLUDED.value,
            value_type = EXCLUDED.value_type,
            updated_by = EXCLUDED.updated_by,
            updated_at = NOW(),
            is_secret  = EXCLUDED.is_secret;
        """
    )

    with mem.engine.begin() as conn:
        for it in items:
            key = it["key"]
            scope = it.get("scope") or "namespace"
            scope_id = it.get("scope_id")
            cat = it.get("category")
            desc = it.get("description")
            overridable = it.get("overridable")
            is_secret = bool(it.get("is_secret", False))

            val_py = it.get("value")
            vtype = it.get("value_type")
            if vtype is None:
                if isinstance(val_py, bool):
                    vtype = "bool"
                elif isinstance(val_py, int):
                    vtype = "int"
                elif isinstance(val_py, (list, dict)):
                    vtype = "json"
                else:
                    vtype = "string"

            try:
                val_json_text = json.dumps(val_py)
            except Exception:
                val_json_text = json.dumps(str(val_py))

            params = {
                "ns": ns,
                "key": key,
                "val": val_json_text,
                "vtype": vtype,
                "scope": scope,
                "scope_id": scope_id,
                "cat": cat,
                "desc": desc,
                "ovr": overridable,
                "upd_by": updated_by,
                "is_secret": is_secret,
            }
            conn.execute(upsert_sql, params)
            written += 1

    return jsonify({"ok": True, "namespace": ns, "written": written})


def infer_type_from_py(v):
    if isinstance(v, bool): return "bool"
    if isinstance(v, int):  return "int"
    if isinstance(v, float):return "float"
    if v is None:           return "json"
    if isinstance(v, (list, dict)): return "json"
    return "string"


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
    data = request.get_json(force=True) or {}
    answered_by = data.get("answered_by") or "unknown"
    admin_reply_text = data.get("admin_reply") or ""

    mem = current_app.config["MEM_ENGINE"]
    rounds = append_admin_note(mem, inq_id, by=answered_by, text_note=admin_reply_text)

    processed = False
    proc_result = None
    if data.get("process"):
        pipeline = current_app.config["PIPELINE"]
        proc_result = pipeline.reprocess_inquiry(inq_id, namespace=pipeline.namespace)
        processed = True

    return jsonify(
        {
            "ok": True,
            "inquiry_id": inq_id,
            "clarification_rounds": rounds,
            "processed": processed,
            "result": proc_result or None,
        }
    ), 200


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
