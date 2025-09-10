from __future__ import annotations
from flask import Blueprint, current_app, request, jsonify
from sqlalchemy import text
import json
from typing import Any, Dict, List

# Make sure the blueprint has this prefix so routes live under /admin...
admin_bp = Blueprint("admin", __name__, url_prefix="/admin")


def _conn():
    # Use the same engine the Pipeline made available
    mem_engine = current_app.config.get("MEM_ENGINE")
    if mem_engine is None:
        raise RuntimeError("MEM_ENGINE not configured on app")
    return mem_engine.connect()


@admin_bp.post("/settings/bulk")
def settings_bulk():
    # expects:
    # {
    #   "namespace": "...",
    #   "updated_by": "...",
    #   "settings": [{ "key": "...", "value": <any>, "scope"?: "namespace"|"global"|"user", "value_type"?: "string"|"int"|"bool"|"json", "is_secret"?: bool }, ...]
    # }
    data = request.get_json(force=True) or {}
    ns = data.get("namespace") or "default"
    updated_by = data.get("updated_by") or "api"
    items: List[Dict[str, Any]] = data.get("settings") or []
    if not items or not isinstance(items, list):
        return jsonify({"ok": False, "error": "settings must be a non-empty array"}), 400

    upsert_sql = text("""
        INSERT INTO mem_settings(
            namespace, key, value, value_type, scope, scope_id,
            category, description, overridable, updated_by, created_at, updated_at, is_secret
        )
        VALUES (
            :ns, :key, :val::jsonb, :vtype, :scope, :scope_id,
            :cat, :desc, COALESCE(:ovr, true), :upd, NOW(), NOW(), COALESCE(:sec, false)
        )
        ON CONFLICT (namespace, key, scope, COALESCE(scope_id, ''))
        DO UPDATE SET
            value = EXCLUDED.value,
            value_type = EXCLUDED.value_type,
            updated_by = EXCLUDED.updated_by,
            updated_at = NOW(),
            is_secret = EXCLUDED.is_secret
    """)

    def infer_value_type(v: Any) -> str:
        if isinstance(v, bool):
            return "bool"
        if isinstance(v, int):
            return "int"
        if isinstance(v, (dict, list)):
            return "json"
        return "string"

    out = {"updated": 0, "namespace": ns}
    with _conn() as c:
        for item in items:
            key = item.get("key")
            if not key:
                continue
            val = item.get("value")
            vtype = item.get("value_type") or infer_value_type(val)
            try:
                val_json = json.dumps(val)
            except Exception:
                val_json = json.dumps(str(val))

            scope = item.get("scope") or "namespace"
            scope_id = item.get("scope_id")
            cat = item.get("category")
            desc = item.get("description")
            ovr = item.get("overridable")
            sec = bool(item.get("is_secret", False))
            c.execute(
                upsert_sql,
                {
                    "ns": ns,
                    "key": key,
                    "val": val_json,
                    "vtype": vtype,
                    "scope": scope,
                    "scope_id": scope_id,
                    "cat": cat,
                    "desc": desc,
                    "ovr": ovr,
                    "upd": updated_by,
                    "sec": sec,
                },
            )
            out["updated"] += 1

    return jsonify({"ok": True, **out})


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


# ----- Admin reply (append note safely & drive the pipeline later) -----

@admin_bp.post("/inquiries/<int:inq_id>/reply")
def admin_reply(inq_id: int):
    """
    Body:
    {
      "answered_by": "admin@example.com",
      "admin_reply": "Use invoices (debtor_trans), date column tran_date, period last month, sum net of credit notes."
    }
    """
    data = request.get_json(force=True) or {}
    by  = (data.get("answered_by") or "").strip() or "admin"
    txt = (data.get("admin_reply") or "").strip()
    if not txt:
        return jsonify({"error": "admin_reply required"}), 400

    # Append to JSONB array WITHOUT casting bound params (use jsonb_build_array/object)
    sql = text("""
        UPDATE mem_inquiries
        SET admin_notes = COALESCE(admin_notes, '[]'::jsonb)
                          || jsonb_build_array(jsonb_build_object('ts', NOW(), 'by', :by, 'text', :txt)),
            clarification_rounds = COALESCE(clarification_rounds, 0) + 1,
            updated_at = NOW()
        WHERE id = :id
        RETURNING id;
    """)
    with _conn() as c:
        r = c.execute(sql, {"id": inq_id, "by": by, "txt": txt}).fetchone()
        if not r:
            return jsonify({"error": "inquiry not found", "inquiry_id": inq_id}), 404

    # You already have core/admin_helpers.py or pipeline hooks – keep those.
    return jsonify({"ok": True, "inquiry_id": inq_id, "appended": True})
