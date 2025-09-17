from flask import Blueprint, request, jsonify
from sqlalchemy import text

from core.sql_exec import get_mem_engine
from core.settings import Settings


def _ensure_mem_settings_conflict_support(conn):
    """
    Ensure a unique index exists on (namespace, key, scope) to support the
    ON CONFLICT clause used by the settings bulk upsert.
    """

    conn.execute(
        text(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS ux_mem_settings_ns_key_scope
            ON mem_settings (namespace, key, scope);
            """
        )
    )


def create_admin_blueprint(settings: Settings) -> Blueprint:
    bp = Blueprint("admin", __name__)

    @bp.post("/settings/bulk")
    def settings_bulk():
        payload = request.get_json(force=True)
        ns = payload.get("namespace") or settings.get("ACTIVE_NAMESPACE", "dw::common")
        updated_by = payload.get("updated_by", "admin")
        items = payload.get("settings") or []

        mem_engine = get_mem_engine(settings)
        upsert_sql = text(
            """
            INSERT INTO mem_settings(namespace, key, value, value_type, scope, scope_id,
                                     overridable, updated_by, created_at, updated_at, is_secret)
            VALUES (:ns, :key, CAST(:val AS jsonb), :vtype, :scope, :scope_id,
                    COALESCE(:ovr, true), :upd_by, NOW(), NOW(), :is_secret)
            ON CONFLICT (namespace, key, scope)
            DO UPDATE SET
              value      = EXCLUDED.value,
              value_type = EXCLUDED.value_type,
              updated_by = EXCLUDED.updated_by,
              updated_at = NOW(),
              is_secret  = EXCLUDED.is_secret
            """
        )

        upserted = 0
        with mem_engine.begin() as conn:
            _ensure_mem_settings_conflict_support(conn)
            for it in items:
                key = it["key"]
                scope = it.get("scope", "namespace")
                scope_id = it.get("scope_id")
                vtype = it.get("value_type")
                is_secret = bool(it.get("is_secret", False))

                val_json = _normalize_setting_value_for_json(it.get("value"), vtype)

                params = {
                    "ns": ns,
                    "key": key,
                    "val": val_json,
                    "vtype": vtype,
                    "scope": scope,
                    "scope_id": scope_id,
                    "ovr": it.get("overridable"),
                    "upd_by": updated_by,
                    "is_secret": is_secret,
                }
                conn.execute(upsert_sql, params)
                upserted += 1

        return jsonify({"ok": True, "namespace": ns, "updated_by": updated_by, "upserted": upserted})

    @bp.get("/settings/get")
    def settings_get():
        ns = request.args.get("namespace") or settings.get("ACTIVE_NAMESPACE", "dw::common")
        keys = request.args.get("keys")
        keys = [k.strip() for k in (keys or "").split(",") if k.strip()]

        mem = get_mem_engine(settings)
        sql = text(
            """
                SELECT id, key, namespace, scope, scope_id, updated_at, value, value_type
                  FROM mem_settings
                 WHERE namespace = :ns
                   AND (:klen = 0 OR key = ANY(:keys))
                 ORDER BY key
            """
        )
        with mem.begin() as conn:
            rows = conn.execute(sql, {"ns": ns, "klen": len(keys), "keys": keys}).mappings().all()

        out = []
        for r in rows:
            out.append(
                {
                    "id": r["id"],
                    "key": r["key"],
                    "namespace": r["namespace"],
                    "scope": r["scope"],
                    "scope_id": r["scope_id"],
                    "updated_at": r["updated_at"],
                    "value": r["value"],
                    "value_type": r["value_type"],
                }
            )
        return jsonify(ok=True, items=out)

    @bp.get("/settings/summary")
    def settings_summary():
        ns = request.args.get("namespace") or settings.get("ACTIVE_NAMESPACE", "dw::common")

        mem = get_mem_engine(settings)
        with mem.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT key, value, value_type, scope
                      FROM mem_settings
                     WHERE namespace = :ns
                     ORDER BY key
                    """
                ),
                {"ns": ns},
            ).mappings().all()

        items = [dict(row) for row in rows]
        return jsonify({"ok": True, "namespace": ns, "items": items})

    return bp


def _normalize_setting_value_for_json(val, vtype):
    """
    Return a JSON string suitable for CAST(:val AS jsonb)
    """

    import json

    if vtype == "string" or isinstance(val, str):
        return json.dumps(val, ensure_ascii=False)
    return json.dumps(val, ensure_ascii=False)
