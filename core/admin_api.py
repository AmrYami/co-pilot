from flask import Blueprint, request, jsonify
from sqlalchemy import text, inspect
import json
import threading

from core.sql_exec import get_mem_engine
from core.settings import Settings


_MEM_SETTINGS_CONSTRAINT_LOCK = threading.Lock()
_MEM_SETTINGS_CONSTRAINT_ENSURED = False


def _ensure_mem_settings_unique_constraint(conn) -> None:
    """Guarantee a non-partial uniqueness guard for (namespace, key, scope, scope_id)."""

    global _MEM_SETTINGS_CONSTRAINT_ENSURED
    if _MEM_SETTINGS_CONSTRAINT_ENSURED:
        return

    with _MEM_SETTINGS_CONSTRAINT_LOCK:
        if _MEM_SETTINGS_CONSTRAINT_ENSURED:
            return

        inspector = inspect(conn)
        if not inspector.has_table("mem_settings"):
            return

        dialect = conn.engine.dialect.name
        if dialect == "postgresql":
            conn.execute(
                text(
                    """
                    DO $$
                    BEGIN
                        ALTER TABLE mem_settings
                            ADD CONSTRAINT uq_mem_settings_ns_key_scope_scopeid
                            UNIQUE (namespace, key, scope, scope_id);
                    EXCEPTION
                        WHEN duplicate_object THEN NULL;
                    END
                    $$;
                    """
                )
            )
        else:
            conn.execute(
                text(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS
                        uq_mem_settings_ns_key_scope_scopeid
                    ON mem_settings(namespace, key, scope, scope_id)
                    """
                )
            )

        _MEM_SETTINGS_CONSTRAINT_ENSURED = True


def create_admin_blueprint(settings: Settings) -> Blueprint:
    bp = Blueprint("admin", __name__)

    @bp.post("/settings/bulk")
    def settings_bulk():
        payload = request.get_json(force=True)
        ns = payload.get("namespace") or settings.get("ACTIVE_NAMESPACE", "dw::common")
        updated_by = payload.get("updated_by", "admin")
        items = payload.get("settings") or []

        mem = get_mem_engine(settings)
        upsert_sql = text(
            """
            INSERT INTO mem_settings(namespace, key, value, value_type, scope, scope_id,
                                     overridable, updated_by, created_at, updated_at, is_secret)
            VALUES (:ns, :key, CAST(:val AS jsonb), :vtype, :scope, :scope_id,
                    true, :upd_by, NOW(), NOW(), :is_secret)
            ON CONFLICT (namespace, key, scope, scope_id)
            DO UPDATE SET
              value      = EXCLUDED.value,
              value_type = EXCLUDED.value_type,
              updated_by = EXCLUDED.updated_by,
              updated_at = NOW(),
              is_secret  = EXCLUDED.is_secret
            """
        )

        upserted = 0
        with mem.begin() as conn:
            _ensure_mem_settings_unique_constraint(conn)
            for it in items:
                key = it["key"]
                vtype = it.get("value_type")
                scope = it.get("scope", "namespace")
                scope_id = it.get("scope_id") or ""
                is_secret = bool(it.get("is_secret", False))

                raw_val = it.get("value")
                if isinstance(raw_val, (dict, list, bool, int, float)) or raw_val is None:
                    val_json = json.dumps(raw_val, ensure_ascii=False)
                else:
                    val_json = json.dumps(raw_val, ensure_ascii=False)

                params = {
                    "ns": ns,
                    "key": key,
                    "val": val_json,
                    "vtype": vtype,
                    "scope": scope,
                    "scope_id": scope_id,
                    "upd_by": updated_by,
                    "is_secret": is_secret,
                }
                conn.execute(upsert_sql, params)
                upserted += 1

        return jsonify(ok=True, namespace=ns, updated_by=updated_by, upserted=upserted)

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

    return bp
