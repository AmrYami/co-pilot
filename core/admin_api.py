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
    """
    Upsert a batch of settings:
    {
      "namespace": "fa::common",
      "updated_by": "amr",
      "settings": [
        {"key": "RESEARCH_MODE", "value": true, "scope": "namespace"},
        {"key": "MAX_CLARIFICATION_ROUNDS", "value": 3, "value_type":"int", "scope": "namespace"}
      ]
    }
    """
    payload = request.get_json(force=True, silent=False) or {}
    namespace = payload.get("namespace") or "default"
    updated_by = payload.get("updated_by") or "system"
    settings: List[Dict[str, Any]] = payload.get("settings") or []

    if not isinstance(settings, list):
        return jsonify({"error": "settings must be a list"}), 400

    # NOTE: we keep one canonical unique index:
    #   (namespace, key, scope, COALESCE(scope_id, ''))
    upsert_sql = text("""
        INSERT INTO mem_settings(namespace, key, value, value_type, scope, scope_id,
                                 category, description, overridable, updated_by, created_at, updated_at, is_secret)
        VALUES (:ns, :key, :val::jsonb, :vtype, :scope, :scope_id,
                :cat, :desc, COALESCE(:ovr, true), :upd, NOW(), NOW(), COALESCE(:sec, false))
        ON CONFLICT (namespace, key, scope, COALESCE(scope_id, ''))
        DO UPDATE SET
            value = EXCLUDED.value,
            value_type = EXCLUDED.value_type,
            updated_by = EXCLUDED.updated_by,
            updated_at = NOW(),
            is_secret = EXCLUDED.is_secret
        ;
    """)

    # Normalize values into json text for val::jsonb
    def _to_json(v: Any) -> str:
        # Already JSON text? keep; else dump
        if isinstance(v, str):
            # If it looks like JSON (starts with { or [ or is a JSON literal), try to validate
            s = v.strip()
            if s.startswith("{") or s.startswith("[") or s in ("true","false","null") or s.replace(".","",1).isdigit():
                try:
                    json.loads(s)
                    return s
                except Exception:
                    return json.dumps(v)
            return json.dumps(v)
        return json.dumps(v)

    out = {"updated": 0, "namespace": namespace}
    with _conn() as c:
        for item in settings:
            key = item.get("key")
            if not key:
                continue
            scope = item.get("scope") or "namespace"
            scope_id = item.get("scope_id")
            vtype = item.get("value_type") or None
            cat = item.get("category")
            desc = item.get("description")
            ovr = item.get("overridable")
            sec = bool(item.get("is_secret", False))
            val_json_text = _to_json(item.get("value"))

            c.execute(upsert_sql, {
                "ns": namespace,
                "key": key,
                "val": val_json_text,
                "vtype": vtype,
                "scope": scope,
                "scope_id": scope_id,
                "cat": cat,
                "desc": desc,
                "ovr": ovr,
                "upd": updated_by,
                "sec": sec,
            })
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
