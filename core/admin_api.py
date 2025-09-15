from __future__ import annotations
from flask import Blueprint, request, jsonify, current_app
from werkzeug.exceptions import BadRequest
from sqlalchemy import text
from core.inquiries import append_admin_note, fetch_inquiry
from core.sql_exec import get_mem_engine
import json

admin_bp = Blueprint("admin", __name__, url_prefix="/admin")


def _json_literal(val) -> str:
    # Always send JSON to the DB; mem_settings.value is jsonb.
    if isinstance(val, (dict, list, bool, int, float)) or val is None:
        return json.dumps(val)
    # Strings as JSON string
    return json.dumps(str(val))


def _infer_value_type(val, explicit: str | None) -> str:
    if explicit:
        return explicit
    if isinstance(val, bool):
        return "bool"
    if isinstance(val, int):
        return "int"
    if isinstance(val, (dict, list)):
        return "json"
    return "string"


def _conn():
    # Use the same engine the Pipeline made available
    mem_engine = current_app.config.get("MEM_ENGINE")
    if mem_engine is None:
        raise RuntimeError("MEM_ENGINE not configured on app")
    return mem_engine.connect()


@admin_bp.post("/settings/bulk")
def settings_bulk():
    payload = request.get_json(force=True) or {}
    ns = payload.get("namespace") or "fa::common"
    updated_by = payload.get("updated_by") or "api"
    items = payload.get("settings") or []

    mem_engine = current_app.config.get("MEM_ENGINE")
    if mem_engine is None:
        return jsonify({"ok": False, "error": "MEM_ENGINE not configured"}), 500

    # Ensure the two partial unique indexes exist
    with mem_engine.begin() as conn:
        conn.execute(text(
            "CREATE UNIQUE INDEX IF NOT EXISTS ux_settings_ns_key_scope_null "
            "ON mem_settings(namespace, key, scope) WHERE scope_id IS NULL"
        ))
        conn.execute(text(
            "CREATE UNIQUE INDEX IF NOT EXISTS ux_settings_ns_key_scope_id "
            "ON mem_settings(namespace, key, scope, scope_id) WHERE scope_id IS NOT NULL"
        ))

    upsert_null = text("""
        INSERT INTO mem_settings(
            namespace, key, value, value_type, scope, scope_id,
            overridable, updated_by, created_at, updated_at, is_secret
        )
        VALUES (
            :ns, :key, CAST(:val AS jsonb), :vtype, :scope, NULL,
            COALESCE(:ovr, true), :upd, NOW(), NOW(), COALESCE(:sec, false)
        )
        ON CONFLICT (namespace, key, scope) WHERE scope_id IS NULL
        DO UPDATE SET
            value      = EXCLUDED.value,
            value_type = EXCLUDED.value_type,
            updated_by = EXCLUDED.updated_by,
            updated_at = NOW(),
            is_secret  = EXCLUDED.is_secret;
    """)

    upsert_scoped = text("""
        INSERT INTO mem_settings(
            namespace, key, value, value_type, scope, scope_id,
            overridable, updated_by, created_at, updated_at, is_secret
        )
        VALUES (
            :ns, :key, CAST(:val AS jsonb), :vtype, :scope, :scope_id,
            COALESCE(:ovr, true), :upd, NOW(), NOW(), COALESCE(:sec, false)
        )
        ON CONFLICT (namespace, key, scope, scope_id) WHERE scope_id IS NOT NULL
        DO UPDATE SET
            value      = EXCLUDED.value,
            value_type = EXCLUDED.value_type,
            updated_by = EXCLUDED.updated_by,
            updated_at = NOW(),
            is_secret  = EXCLUDED.is_secret;
    """)

    upserted = 0
    with mem_engine.begin() as conn:
        for it in items:
            key = it["key"]
            scope = it.get("scope") or "namespace"
            scope_id = it.get("scope_id")
            vtype = _infer_value_type(it.get("value"), it.get("value_type"))
            val = _json_literal(it.get("value"))
            params = {
                "ns": ns,
                "key": key,
                "val": val,
                "vtype": vtype,
                "scope": scope,
                "scope_id": scope_id,
                "ovr": it.get("overridable"),
                "upd": updated_by,
                "sec": bool(it.get("is_secret", False)),
            }
            if scope_id is None or scope_id == "":
                conn.execute(upsert_null, params)
            else:
                conn.execute(upsert_scoped, params)
            upserted += 1

    return jsonify({"ok": True, "namespace": ns, "updated_by": updated_by, "upserted": upserted})


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
    answered_by = data.get("answered_by") or "admin@local"
    admin_reply_txt = (data.get("admin_reply") or "").strip()
    process = bool(data.get("process"))

    pipeline = current_app.config["PIPELINE"]
    mem = get_mem_engine(pipeline.settings)
    rounds = append_admin_note(mem, inq_id, by=answered_by, text_note=admin_reply_txt)

    result = {"ok": True, "inquiry_id": inq_id, "clarification_rounds": rounds}

    if process:
        proc = pipeline.reprocess_inquiry(inq_id, namespace=pipeline.namespace)
        result.update({"processed": True, "result": proc})

    return jsonify(result)


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

# helper

 
