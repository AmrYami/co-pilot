from __future__ import annotations
from flask import Blueprint, request, jsonify, current_app
from sqlalchemy import text, bindparam
from sqlalchemy.dialects.postgresql import JSONB
from core.sql_exec import get_mem_engine

# Make sure the blueprint has this prefix so routes live under /admin...
admin_bp = Blueprint("admin", __name__, url_prefix="/admin")


def _conn():
    # Use the same engine the Pipeline made available
    mem_engine = current_app.config.get("MEM_ENGINE")
    if mem_engine is None:
        raise RuntimeError("MEM_ENGINE not configured on app")
    return mem_engine.connect()


def _infer_value_type(val, explicit: str | None) -> str:
    if explicit:
        return explicit
    if isinstance(val, bool):
        return "bool"
    if isinstance(val, int) and not isinstance(val, bool):
        return "int"
    if isinstance(val, (dict, list)):
        return "json"
    return "string"


@admin_bp.post("/settings/bulk")
def settings_bulk():
    payload = request.get_json(force=True) or {}
    ns = payload.get("namespace") or "fa::common"
    who = payload.get("updated_by") or "api"
    items = payload.get("settings") or []
    if not isinstance(items, list) or not items:
        return jsonify({"ok": False, "error": "settings list required"}), 400

    mem = get_mem_engine(current_app.config.get("MEM_ENGINE"))

    upsert_sql = text("""
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
            value = EXCLUDED.value,
            value_type = EXCLUDED.value_type,
            updated_by = EXCLUDED.updated_by,
            updated_at = NOW(),
            is_secret = EXCLUDED.is_secret
    """).bindparams(bindparam("val", type_=JSONB))

    results = []
    with mem.begin() as c:
        for it in items:
            key = it.get("key")
            if not key:
                results.append({"key": None, "ok": False, "error": "missing key"})
                continue

            value = it.get("value")
            vtype = _infer_value_type(value, it.get("value_type"))
            scope = it.get("scope") or "namespace"
            scope_id = it.get("scope_id")
            is_secret = bool(it.get("is_secret", False))
            overr = it.get("overridable")
            cat = it.get("category")
            desc = it.get("description")

            params = {
                "ns": ns,
                "key": key,
                "val": value,
                "vtype": vtype,
                "scope": scope,
                "scope_id": scope_id,
                "cat": cat,
                "desc": desc,
                "ovr": overr,
                "upd": who,
                "sec": is_secret,
            }

            try:
                c.execute(upsert_sql, params)
                results.append({"key": key, "ok": True})
            except Exception as e:
                results.append({"key": key, "ok": False, "error": str(e)})

    return jsonify({"ok": True, "namespace": ns, "updated_by": who, "results": results})


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
