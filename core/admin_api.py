from __future__ import annotations
from flask import Blueprint, request, jsonify, current_app
from werkzeug.exceptions import BadRequest
from sqlalchemy import text
from core.inquiries import append_admin_note, fetch_inquiry
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
    payload = request.get_json(force=True) or {}
    ns = payload.get("namespace") or "fa::common"
    updated_by = payload.get("updated_by") or "api"
    items = payload.get("settings") or []

    pipeline = current_app.config.get("PIPELINE")
    mem_engine = None
    if pipeline is not None:
        mem_engine = getattr(pipeline, "mem_engine", None)
    if mem_engine is None:
        mem_engine = current_app.config.get("MEM_ENGINE")
    if mem_engine is None:
        return jsonify({"ok": False, "error": "MEM_ENGINE not configured on app"}), 500

    def _infer_vtype(v):
        if isinstance(v, bool):  return "bool"
        if isinstance(v, int):   return "int"
        if isinstance(v, float): return "float"
        if isinstance(v, (list, dict)): return "json"
        return "string"

    results = {"ok": True, "upserted": 0, "updated_by": updated_by, "namespace": ns, "items": []}
    with mem_engine.begin() as conn:
        for it in items:
            key = it["key"]
            raw_val = it.get("value")
            scope = it.get("scope", "namespace")
            scope_id = it.get("scope_id")
            vtype = it.get("value_type") or _infer_vtype(raw_val)
            cat = it.get("category")
            desc = it.get("description")
            ovr = it.get("overridable")
            is_secret = bool(it.get("is_secret", False))

            val_json = json.dumps(raw_val, ensure_ascii=False)

            upsert_sql = text("""
                WITH upd AS (
                    UPDATE mem_settings
                       SET value      = CAST(:val AS jsonb),
                           value_type = :vtype,
                           updated_by = :upd_by,
                           updated_at = NOW(),
                           is_secret  = COALESCE(:is_secret, false),
                           category   = COALESCE(:cat, category),
                           description= COALESCE(:desc, description),
                           overridable= COALESCE(:ovr, overridable)
                     WHERE namespace = :ns
                       AND key       = :key
                       AND scope     = :scope
                       AND (
                             (:scope_id IS NULL AND scope_id IS NULL)
                          OR (scope_id = :scope_id)
                           )
                 RETURNING id
                )
                INSERT INTO mem_settings (
                    namespace, key, value, value_type, scope, scope_id,
                    category, description, overridable, updated_by,
                    created_at, updated_at, is_secret
                )
                SELECT :ns, :key, CAST(:val AS jsonb), :vtype, :scope, :scope_id,
                       :cat, :desc, COALESCE(:ovr, true), :upd_by,
                       NOW(), NOW(), COALESCE(:is_secret, false)
                WHERE NOT EXISTS (SELECT 1 FROM upd);
            """)

            conn.execute(
                upsert_sql,
                {
                    "ns": ns, "key": key, "val": val_json,
                    "vtype": vtype, "scope": scope, "scope_id": scope_id,
                    "cat": cat, "desc": desc, "ovr": ovr, "upd_by": updated_by,
                    "is_secret": is_secret,
                },
            )
            results["upserted"] += 1
            results["items"].append({"key": key, "scope": scope, "scope_id": scope_id})

    return jsonify(results)


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
