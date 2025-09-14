from __future__ import annotations
from flask import Blueprint, request, jsonify, current_app
from werkzeug.exceptions import BadRequest
from sqlalchemy import text
from core.inquiries import append_admin_note, fetch_inquiry
from core.settings import Settings
from core.sql_exec import get_mem_engine
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
    """
    Upsert a batch of settings into mem_settings.
    Expects JSON:
    {
      "namespace": "fa::common",
      "updated_by": "amr",
      "settings": [
         {"key": "...", "value": <any json>, "scope": "namespace|global|user", "scope_id": null|"...",
          "value_type": "string|int|bool|json", "category": "...", "description": "...", "is_secret": false, "overridable": true}
      ]
    }
    """
    payload = request.get_json(force=True, silent=False) or {}
    ns = payload.get("namespace") or "default"
    updated_by = payload.get("updated_by") or "api"
    items = payload.get("settings") or []

    mem_engine = get_mem_engine(Settings())  # reuse pool
    results = []
    errors = []

    # Decide a value_type if not provided
    def infer_value_type(v) -> str:
        if isinstance(v, bool):  return "bool"
        if isinstance(v, int):   return "int"
        if isinstance(v, float): return "float"
        if isinstance(v, (list, dict)): return "json"
        return "string"

    # Two upsert statements (one for NULL scope_id, one for non-null)
    UPSERT_NULL = text("""
        INSERT INTO mem_settings(
            namespace, key, value, value_type, scope, scope_id,
            category, description, overridable, updated_by, created_at, updated_at, is_secret
        )
        VALUES (
            :ns, :key, CAST(:val_json AS jsonb), :vtype, :scope, NULL,
            :cat, :desc, COALESCE(:ovr, true), :upd_by, NOW(), NOW(), COALESCE(:is_secret, false)
        )
        ON CONFLICT (namespace, key, scope) WHERE scope_id IS NULL
        DO UPDATE SET
            value      = EXCLUDED.value,
            value_type = EXCLUDED.value_type,
            updated_by = EXCLUDED.updated_by,
            updated_at = NOW(),
            is_secret  = EXCLUDED.is_secret;
    """)

    UPSERT_SCOPED = text("""
        INSERT INTO mem_settings(
            namespace, key, value, value_type, scope, scope_id,
            category, description, overridable, updated_by, created_at, updated_at, is_secret
        )
        VALUES (
            :ns, :key, CAST(:val_json AS jsonb), :vtype, :scope, :scope_id,
            :cat, :desc, COALESCE(:ovr, true), :upd_by, NOW(), NOW(), COALESCE(:is_secret, false)
        )
        ON CONFLICT (namespace, key, scope, scope_id)
        DO UPDATE SET
            value      = EXCLUDED.value,
            value_type = EXCLUDED.value_type,
            updated_by = EXCLUDED.updated_by,
            updated_at = NOW(),
            is_secret  = EXCLUDED.is_secret;
    """)

    with mem_engine.begin() as conn:
        for it in items:
            key = it.get("key")
            if not key:
                errors.append({"key": None, "error": "missing key"})
                continue

            val = it.get("value")
            val_type = it.get("value_type") or infer_value_type(val)
            scope = (it.get("scope") or "namespace").lower()
            scope_id = it.get("scope_id")  # may be None
            cat = it.get("category")
            desc = it.get("description")
            is_secret = bool(it.get("is_secret", False))
            overridable = it.get("overridable")
            # IMPORTANT: always JSON-encode before CAST(:val_json AS jsonb)
            val_json = json.dumps(val)

            params = {
                "ns": ns,
                "key": key,
                "val_json": val_json,
                "vtype": val_type,
                "scope": scope,
                "scope_id": scope_id,
                "cat": cat,
                "desc": desc,
                "ovr": overridable,
                "upd_by": updated_by,
                "is_secret": is_secret,
            }

            try:
                if scope_id is None:
                    conn.execute(UPSERT_NULL, params)
                else:
                    conn.execute(UPSERT_SCOPED, params)
                results.append({"key": key, "ok": True})
            except Exception as e:
                errors.append({"key": key, "error": f"{type(e).__name__}: {e}"})

    return jsonify({"ok": len(errors) == 0, "updated_by": updated_by, "namespace": ns,
                    "results": results, "errors": errors}), (200 if not errors else 207)


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
