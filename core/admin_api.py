from __future__ import annotations
from flask import Blueprint, request, jsonify, current_app
from werkzeug.exceptions import BadRequest
from sqlalchemy import text
import json
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
    mem_engine = current_app.config["MEM_ENGINE"]
    payload = request.get_json(force=True) or {}
    ns = payload.get("namespace") or "fa::common"
    updated_by = payload.get("updated_by") or "api"

    CASTS = {
        "json": "CAST(:val AS jsonb)",
        "int": "CAST(:val AS integer)",
        "bool": "CAST(:val AS boolean)",
        "string": "CAST(:val AS text)",
        None: "CAST(:val AS jsonb)",
    }

    def _infer_value_type(v):
        if isinstance(v, bool):
            return "bool"
        if isinstance(v, int):
            return "int"
        if isinstance(v, (list, dict)):
            return "json"
        return "string"

    items = payload.get("settings") or []
    updated = []

    with mem_engine.begin() as c:
        for it in items:
            key = it["key"]
            scope = it.get("scope", "namespace")
            scope_id = it.get("scope_id")
            val = it.get("value")
            vtype = it.get("value_type") or _infer_value_type(val)
            is_secret = bool(it.get("is_secret", False))

            if vtype == "json":
                bound_val = json.dumps(val)
            elif vtype == "bool":
                bound_val = str(bool(val)).lower()
            elif vtype == "int":
                bound_val = int(val) if val is not None else None
            else:
                bound_val = "" if val is None else str(val)

            value_expr = CASTS.get(vtype, CASTS[None])

            upsert_sql = text(
                f"""
            INSERT INTO mem_settings(namespace, key, value, value_type, scope, scope_id,
                                     category, description, overridable, updated_by, created_at, updated_at, is_secret)
            VALUES (:ns, :key, {value_expr}, :vtype, :scope, :scope_id,
                    :cat, :desc, COALESCE(:ovr, true), :upd_by, NOW(), NOW(), :is_secret)
            ON CONFLICT (namespace, key, scope, COALESCE(scope_id, ''))
            DO UPDATE SET
                value      = EXCLUDED.value,
                value_type = EXCLUDED.value_type,
                updated_by = EXCLUDED.updated_by,
                updated_at = NOW(),
                is_secret  = EXCLUDED.is_secret
            """
            )

            c.execute(
                upsert_sql,
                {
                    "ns": ns,
                    "key": key,
                    "val": bound_val,
                    "vtype": vtype,
                    "scope": scope,
                    "scope_id": scope_id,
                    "cat": it.get("category"),
                    "desc": it.get("description"),
                    "ovr": it.get("overridable"),
                    "upd_by": updated_by,
                    "is_secret": is_secret,
                },
            )
            updated.append(
                {"key": key, "value": val, "value_type": vtype, "scope": scope}
            )

    return {"ok": True, "items": updated}, 200


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
    payload = request.get_json(force=True) or {}
    answered_by = payload.get("answered_by") or "unknown"
    admin_reply_text = payload.get("admin_reply") or ""
    process = bool(payload.get("process"))

    pipeline = current_app.config["PIPELINE"]
    mem = pipeline.mem_engine

    try:
        rounds = append_admin_note(
            mem,
            inq_id,
            by=answered_by,
            text_note=admin_reply_text,
            admin_reply=admin_reply_text,
        )
    except Exception as e:
        return jsonify({"ok": False, "error": f"append_failed: {e}"}), 500

    result = {
        "ok": True,
        "inquiry_id": inq_id,
        "clarification_rounds": rounds,
    }

    if process:
        try:
            proc = pipeline.process_inquiry(inq_id)
            result["processed"] = True
            result["result"] = proc
        except Exception as e:
            return jsonify({"ok": False, "inquiry_id": inq_id, "error": f"process_failed: {e}"}), 500

    return jsonify(result), 200


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
