from __future__ import annotations
from flask import Blueprint, request, jsonify, current_app
from werkzeug.exceptions import BadRequest
from sqlalchemy.sql import text, bindparam
from sqlalchemy.dialects.postgresql import JSONB
from core.inquiries import append_admin_note, fetch_inquiry

admin_bp = Blueprint("admin", __name__, url_prefix="/admin")


def _conn():
    # Use the same engine the Pipeline made available
    mem_engine = current_app.config.get("MEM_ENGINE")
    if mem_engine is None:
        raise RuntimeError("MEM_ENGINE not configured on app")
    return mem_engine.connect()


@admin_bp.post("/settings/bulk")
def settings_bulk():
    data = request.get_json(force=True) or {}
    ns = data.get("namespace") or "fa::common"
    updated_by = data.get("updated_by") or "api"
    items = data.get("settings") or []

    inserted, updated = 0, 0
    problems: list[dict] = []

    upsert_sql = text(
        """
        INSERT INTO mem_settings(
            namespace, key, value, value_type, scope, scope_id,
            category, description, overridable, updated_by, created_at, updated_at, is_secret
        )
        VALUES (
            :ns, :key, :val, :vtype, :scope, :scope_id,
            :cat, :desc, COALESCE(:ovr, true), :upd_by, NOW(), NOW(), COALESCE(:is_secret, false)
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

    def infer_vtype(py_val) -> str:
        # Store scalars as JSON too (string/bool/int appear as JSON, e.g. "fa", true, 587)
        if isinstance(py_val, bool):
            return "bool"
        if isinstance(py_val, int):
            return "int"
        if isinstance(py_val, float):
            return "float"
        if isinstance(py_val, (list, dict)):
            return "json"
        return "string"  # str/None/other -> JSON string or null

    engine = current_app.config["MEM_ENGINE"]
    with engine.begin() as c:
        for item in items:
            key = item.get("key")
            scope = item.get("scope") or "namespace"
            scope_id = item.get("scope_id")
            raw_val = item.get("value")
            vtype = item.get("value_type") or infer_vtype(raw_val)
            is_secret = bool(item.get("is_secret") or False)

            try:
                c.execute(
                    upsert_sql,
                    {
                        "ns": ns,
                        "key": key,
                        "val": raw_val,
                        "vtype": vtype,
                        "scope": scope,
                        "scope_id": scope_id,
                        "cat": item.get("category"),
                        "desc": item.get("description"),
                        "ovr": item.get("overridable"),
                        "upd_by": updated_by,
                        "is_secret": is_secret,
                    },
                )
                updated += 1
            except Exception as e:
                problems.append({"key": key, "error": str(e)})

    return {
        "ok": len(problems) == 0,
        "namespace": ns,
        "updated_by": updated_by,
        "total": len(items),
        "applied": updated,
        "errors": problems,
    }, (200 if not problems else 207)


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
