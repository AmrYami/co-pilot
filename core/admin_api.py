from __future__ import annotations

import json

from flask import Blueprint, current_app, jsonify, request
from sqlalchemy import text
from werkzeug.exceptions import BadRequest

from core.inquiries import append_admin_note, fetch_inquiry
from core.settings import Settings
from core.sql_exec import get_mem_engine

admin_bp = Blueprint("admin", __name__, url_prefix="/admin")


def _mem_engine():
    mem_engine = current_app.config.get("MEM_ENGINE")
    if mem_engine is not None:
        return mem_engine

    pipeline = current_app.config.get("PIPELINE") or getattr(current_app, "pipeline", None)
    if pipeline is not None:
        candidate = getattr(pipeline, "mem_engine", None) or getattr(pipeline, "mem", None)
        if candidate is not None:
            current_app.config["MEM_ENGINE"] = candidate
            return candidate

    settings_obj = current_app.config.get("SETTINGS")
    if isinstance(settings_obj, Settings):
        mem_engine = get_mem_engine(settings_obj)
    else:
        mem_engine = get_mem_engine(Settings())

    current_app.config["MEM_ENGINE"] = mem_engine
    return mem_engine


def _conn():
    return _mem_engine().connect()


def _infer_vtype(value) -> str:
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "float"
    if isinstance(value, (dict, list)):
        return "json"
    return "string"


@admin_bp.post("/settings/bulk")
def settings_bulk():
    payload = request.get_json(force=True) or {}
    namespace = (payload.get("namespace") or "dw::common").strip()
    updated_by = payload.get("updated_by") or "admin"
    items = payload.get("settings") or []

    mem_engine = _mem_engine()

    upsert_sql = text(
        """
        WITH up AS (
            UPDATE mem_settings
               SET value      = CAST(:val AS jsonb),
                   value_type = :vtype,
                   category   = COALESCE(:cat, category),
                   description= COALESCE(:desc, description),
                   overridable= COALESCE(:ovr, overridable),
                   updated_by = :upd,
                   updated_at = NOW(),
                   is_secret  = :sec
             WHERE namespace=:ns
               AND key=:key
               AND scope=:scope
               AND COALESCE(scope_id,'') = COALESCE(:scope_id,'')
         RETURNING 1
        )
        INSERT INTO mem_settings(
            namespace, key, value, value_type, scope, scope_id,
            category, description, overridable, updated_by, created_at, updated_at, is_secret
        )
        SELECT :ns, :key, CAST(:val AS jsonb), :vtype, :scope, :scope_id,
               :cat, :desc, COALESCE(:ovr, true), :upd, NOW(), NOW(), :sec
        WHERE NOT EXISTS (SELECT 1 FROM up);
        """
    )

    upserted = 0
    with mem_engine.begin() as conn:
        for item in items:
            key = item.get("key")
            if not key:
                continue

            value = item.get("value")
            vtype = item.get("value_type") or _infer_vtype(value)
            scope = (item.get("scope") or "namespace").strip() or "namespace"
            scope_id = item.get("scope_id")
            if scope_id == "":
                scope_id = None

            params = {
                "ns": namespace,
                "key": key,
                "val": json.dumps(value, ensure_ascii=False),
                "vtype": vtype,
                "scope": scope,
                "scope_id": scope_id,
                "cat": item.get("category"),
                "desc": item.get("description"),
                "ovr": item.get("overridable"),
                "upd": updated_by,
                "sec": bool(item.get("is_secret", False)),
            }
            conn.execute(upsert_sql, params)
            upserted += 1

    return jsonify({"ok": True, "namespace": namespace, "updated_by": updated_by, "upserted": upserted})


@admin_bp.get("/settings/get")
def settings_get():
    """Quick fetch: /admin/settings/get?namespace=dw::common&key=RESEARCH_MODE"""
    ns = request.args.get("namespace", "default")
    key = request.args.get("key")
    where = "WHERE namespace = :ns"
    params = {"ns": ns}
    if key:
        where += " AND key = :k"
        params["k"] = key
    sql = text(
        f"SELECT id, namespace, key, value, value_type, scope, scope_id, updated_at "
        f"FROM mem_settings {where} ORDER BY key;"
    )
    with _conn() as c:
        rows = [dict(r._mapping) for r in c.execute(sql, params)]
    return jsonify({"ok": True, "items": rows})


# ----- Admin reply (append note safely & drive the pipeline) -----


@admin_bp.get("/inquiries/<int:inq_id>")
def get_inquiry(inq_id: int):
    mem = _mem_engine()
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
