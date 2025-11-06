from __future__ import annotations

import json

from flask import Blueprint, abort, jsonify, request
from sqlalchemy import bindparam, text

from core.settings import Settings
from core.sql_exec import get_mem_engine

admin_bp = Blueprint("admin", __name__)


def _manual_upsert_setting(
    conn,
    *,
    ns: str,
    key: str,
    value_json: str,
    value_type: str,
    scope: str,
    scope_id,
    updated_by: str,
    is_secret: bool = False,
) -> None:
    update_stmt = text(
        """
        UPDATE mem_settings
           SET value = CAST(:val AS jsonb),
               value_type = :vtype,
               updated_by = :upd_by,
               updated_at = NOW(),
               is_secret  = :secret
         WHERE namespace = :ns
           AND key       = :key
           AND scope     = :scope
           AND ((:scope_id IS NULL AND scope_id IS NULL) OR scope_id = :scope_id)
        """
    )
    result = conn.execute(
        update_stmt,
        {
            "ns": ns,
            "key": key,
            "val": value_json,
            "vtype": value_type,
            "scope": scope,
            "scope_id": scope_id,
            "upd_by": updated_by,
            "secret": is_secret,
        },
    )
    if result.rowcount and result.rowcount > 0:
        return

    insert_stmt = text(
        """
        INSERT INTO mem_settings(namespace, key, value, value_type, scope, scope_id,
                                 overridable, updated_by, created_at, updated_at, is_secret)
        VALUES (:ns, :key, CAST(:val AS jsonb), :vtype, :scope, :scope_id,
                true, :upd_by, NOW(), NOW(), :secret)
        """
    )
    conn.execute(
        insert_stmt,
        {
            "ns": ns,
            "key": key,
            "val": value_json,
            "vtype": value_type,
            "scope": scope,
            "scope_id": scope_id,
            "upd_by": updated_by,
            "secret": is_secret,
        },
    )


def _infer_value_type(value) -> str:
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int) and not isinstance(value, bool):
        return "int"
    if isinstance(value, float):
        return "float"
    if isinstance(value, (list, dict)):
        return "json"
    return "string"


@admin_bp.post("/settings/bulk")
def settings_bulk():
    payload = request.get_json(force=True) or {}
    ns = payload.get("namespace") or "default"
    updated_by = payload.get("updated_by") or "admin"

    settings_items: list[dict] = []

    raw_settings = payload.get("settings")
    if isinstance(raw_settings, list):
        for entry in raw_settings:
            if not isinstance(entry, dict):
                abort(400, description="Each settings item must be an object")
            if "key" in entry:
                settings_items.append(dict(entry))
                continue
            if len(entry) != 1:
                abort(400, description="Settings item missing 'key'")
            key, value = next(iter(entry.items()))
            if isinstance(value, dict) and "value" in value:
                item = dict(value)
                item.setdefault("key", key)
            else:
                item = {"key": key, "value": value}
            settings_items.append(item)
    elif isinstance(raw_settings, dict):
        for key, entry in raw_settings.items():
            if isinstance(entry, dict) and "value" in entry:
                item = dict(entry)
                item.setdefault("key", key)
            else:
                item = {"key": key, "value": entry}
            settings_items.append(item)

    overrides = payload.get("overrides")
    if isinstance(overrides, dict):
        for key, entry in overrides.items():
            if isinstance(entry, dict) and "value" in entry:
                item = dict(entry)
                item.setdefault("key", key)
            else:
                item = {"key": key, "value": entry}
            settings_items.append(item)

    if not settings_items:
        return jsonify({"ok": True, "namespace": ns, "upserted": 0})

    settings = Settings(namespace=ns)
    mem = get_mem_engine(settings)

    with mem.begin() as conn:
        for item in settings_items:
            if "key" not in item:
                abort(400, description="Missing 'key' in settings payload")
            key = item["key"]
            scope = item.get("scope") or "namespace"
            scope_id = item.get("scope_id")
            value = item.get("value")
            value_type = item.get("value_type") or _infer_value_type(value)
            is_secret = bool(item.get("is_secret"))

            value_json = json.dumps(value, ensure_ascii=False)

            _manual_upsert_setting(
                conn,
                ns=ns,
                key=key,
                value_json=value_json,
                value_type=value_type,
                scope=scope,
                scope_id=scope_id,
                updated_by=updated_by,
                is_secret=is_secret,
            )

    return jsonify({"ok": True, "namespace": ns, "upserted": len(settings_items)})


@admin_bp.get("/settings/get")
def settings_get():
    ns = request.args.get("namespace") or "default"
    keys_param = request.args.get("keys")
    keys = [k for k in (keys_param.split(",") if keys_param else []) if k]

    settings = Settings(namespace=ns)
    mem = get_mem_engine(settings)

    with mem.begin() as conn:
        if keys:
            stmt = (
                text(
                    """
                    SELECT key, value, value_type, scope, scope_id
                      FROM mem_settings
                     WHERE namespace = :ns
                       AND key IN :keys
                    """
                ).bindparams(bindparam("keys", expanding=True))
            )
            rows = conn.execute(stmt, {"ns": ns, "keys": keys}).mappings().all()
        else:
            rows = conn.execute(
                text(
                    """
                    SELECT key, value, value_type, scope, scope_id
                      FROM mem_settings
                     WHERE namespace = :ns
                    """
                ),
                {"ns": ns},
            ).mappings().all()

    items = [dict(row) for row in rows]
    return jsonify({"ok": True, "namespace": ns, "items": items})


@admin_bp.get("/settings/summary")
def settings_summary():
    ns = request.args.get("namespace") or "default"
    settings = Settings(namespace=ns)
    mem = get_mem_engine(settings)

    with mem.begin() as conn:
        total = conn.execute(
            text("SELECT COUNT(*) FROM mem_settings WHERE namespace = :ns"),
            {"ns": ns},
        ).scalar_one()
        keys = conn.execute(
            text(
                """
                SELECT DISTINCT key
                  FROM mem_settings
                 WHERE namespace = :ns
              ORDER BY key
                """
            ),
            {"ns": ns},
        ).scalars().all()

    return jsonify({"ok": True, "namespace": ns, "total": total, "keys": keys})


def create_admin_blueprint(settings: Settings | None = None) -> Blueprint:
    return admin_bp
