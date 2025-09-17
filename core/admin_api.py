from __future__ import annotations

import json

from flask import Blueprint, request
from sqlalchemy import text

from core.settings import Settings
from core.sql_exec import get_mem_engine

admin_bp = Blueprint("admin", __name__)


def _ensure_mem_settings_unique_constraint(conn) -> None:
    conn.execute(
        text(
            """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_indexes
                 WHERE tablename = 'mem_settings'
                   AND indexname = 'ux_settings_ns_key_scope_null'
            ) THEN
                CREATE UNIQUE INDEX ux_settings_ns_key_scope_null
                  ON mem_settings (namespace, key, scope)
                 WHERE scope_id IS NULL;
            END IF;

            IF NOT EXISTS (
                SELECT 1 FROM pg_indexes
                 WHERE tablename = 'mem_settings'
                   AND indexname = 'ux_settings_ns_key_scope_id'
            ) THEN
                CREATE UNIQUE INDEX ux_settings_ns_key_scope_id
                  ON mem_settings (namespace, key, scope, scope_id)
                 WHERE scope_id IS NOT NULL;
            END IF;
        END$$;
        """
        )
    )


@admin_bp.route("/admin/settings/bulk", methods=["POST"])
def settings_bulk():
    payload = request.get_json(force=True)
    ns = payload.get("namespace", "dw::common")
    updated_by = payload.get("updated_by", "admin")
    items = payload.get("settings", [])

    s = Settings(namespace=ns)
    mem = get_mem_engine(s)

    with mem.begin() as conn:
        _ensure_mem_settings_unique_constraint(conn)
        for item in items:
            key = item["key"]
            val = item.get("value")
            vtype = item.get("value_type")
            scope = item.get("scope", "namespace")
            scope_id = item.get("scope_id")
            is_secret = bool(item.get("is_secret", False))

            if scope_id in (None, ""):
                stmt = text(
                    """
                    INSERT INTO mem_settings(namespace, key, value, value_type, scope, scope_id,
                                             overridable, updated_by, created_at, updated_at, is_secret)
                    VALUES (:ns, :key, CAST(:val AS jsonb), :vtype, :scope, NULL,
                            true, :upd, NOW(), NOW(), :is_secret)
                    ON CONFLICT ON CONSTRAINT ux_settings_ns_key_scope_null
                    DO UPDATE SET
                      value      = EXCLUDED.value,
                      value_type = EXCLUDED.value_type,
                      updated_by = EXCLUDED.updated_by,
                      updated_at = NOW(),
                      is_secret  = EXCLUDED.is_secret
                    """
                )
            else:
                stmt = text(
                    """
                    INSERT INTO mem_settings(namespace, key, value, value_type, scope, scope_id,
                                             overridable, updated_by, created_at, updated_at, is_secret)
                    VALUES (:ns, :key, CAST(:val AS jsonb), :vtype, :scope, :scope_id,
                            true, :upd, NOW(), NOW(), :is_secret)
                    ON CONFLICT ON CONSTRAINT ux_settings_ns_key_scope_id
                    DO UPDATE SET
                      value      = EXCLUDED.value,
                      value_type = EXCLUDED.value_type,
                      updated_by = EXCLUDED.updated_by,
                      updated_at = NOW(),
                      is_secret  = EXCLUDED.is_secret
                    """
                )

            conn.execute(
                stmt,
                {
                    "ns": ns,
                    "key": key,
                    "val": json.dumps(val),
                    "vtype": vtype,
                    "scope": scope,
                    "scope_id": scope_id,
                    "upd": updated_by,
                    "is_secret": is_secret,
                },
            )

    return {"ok": True, "namespace": ns, "upserted": len(items)}


def create_admin_blueprint(settings: Settings | None = None) -> Blueprint:
    return admin_bp
