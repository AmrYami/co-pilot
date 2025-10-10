"""Utilities for reading DW namespace settings with safe defaults."""
from __future__ import annotations

import json
import logging
import os
from functools import lru_cache
from typing import Any, Dict, Iterable, List

try:
    from sqlalchemy import create_engine, text
except Exception:  # pragma: no cover - optional dependency at runtime
    create_engine = None
    text = None

from core.settings import Settings

_NAMESPACE = "dw::common"


def _coerce(value: Any, value_type: str) -> Any:
    value_type_norm = (value_type or "").lower()
    if value_type_norm == "json":
        if isinstance(value, (dict, list)):
            return value
        try:
            return json.loads(value)
        except Exception:
            return {}
    if value_type_norm == "int":
        try:
            return int(value)
        except Exception:
            return 0
    if value_type_norm == "bool":
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in {"1", "true", "t", "yes", "y"}
    return value


@lru_cache(maxsize=1)
def get_settings() -> Dict[str, Any]:
    """Load dw::common settings from the memory DB with a file fallback."""

    settings_map: Dict[str, Any] = {}
    db_url = os.getenv("MEMORY_DB_URL", "").strip()
    rows = []

    if db_url and create_engine and text:
        try:
            engine = create_engine(db_url, pool_pre_ping=True, future=True)
            with engine.begin() as conn:
                rows = (
                    conn.execute(
                        text(
                            """
                            SELECT key, value, value_type, scope
                            FROM mem_settings
                            WHERE namespace = :ns
                            ORDER BY key
                            """
                        ),
                        {"ns": _NAMESPACE},
                    )
                    .mappings()
                    .all()
                )
        except Exception as exc:  # pragma: no cover - defensive fallback
            logging.warning("get_settings(): DB load failed: %s", exc)

    if not rows:
        snapshot_path = os.path.join(os.getcwd(), "docs", "state", "settings_export.json")
        try:
            with open(snapshot_path, "r", encoding="utf-8") as handler:
                snapshot = json.load(handler)
                if isinstance(snapshot, list):
                    rows = snapshot
        except Exception:
            rows = []

    for row in rows:
        key = row.get("key") if isinstance(row, dict) else None
        if not key:
            continue
        settings_map[key] = _coerce(row.get("value"), row.get("value_type"))

    settings_map.setdefault("DW_FTS_ENGINE", "like")
    return settings_map


def get_namespace_json(db: Any, key: str, default: Any) -> Any:
    """Return a JSON-like configuration value for the given key.

    ``db`` can be a settings object with ``fetch_setting``/``get_json``/``get`` methods or a
    plain mapping already containing the namespace data. The function tolerates absent keys
    by returning ``default``.
    """

    if db is None:
        return default

    # 1) Explicit fetch_setting hook (preferred by caller when available)
    fetch = getattr(db, "fetch_setting", None)
    if callable(fetch):
        try:
            row = fetch(key, scope="namespace")
        except TypeError:
            row = fetch(key)
        if row and isinstance(row, dict) and "value" in row:
            value = row.get("value")
            if value is not None:
                return value

    # 2) get_json / get style accessors
    for attr in ("get_json", "get"):
        getter = getattr(db, attr, None)
        if callable(getter):
            for kwargs in (
                {"default": default, "scope": "namespace"},
                {"default": default},
                {"scope": "namespace"},
                {},
            ):
                try:
                    value = getter(key, **kwargs)
                except TypeError:
                    continue
                if value is not None:
                    return value

    # 3) Mapping-like objects
    if isinstance(db, dict):
        value = db.get(key, default)
        return value if value is not None else default

    return default


def _normalize_columns(raw: Iterable[Any]) -> List[str]:
    seen: set[str] = set()
    out: List[str] = []
    for col in raw:
        if not isinstance(col, str):
            continue
        norm = col.strip().strip('"')
        if not norm:
            continue
        up = norm.upper()
        if up not in seen:
            seen.add(up)
            out.append(up)
    return out


def get_fts_columns(db: Any, table: str) -> List[str]:
    """Return the configured FTS columns for ``table`` with sensible defaults."""

    cfg = get_namespace_json(db, "DW_FTS_COLUMNS", default={})
    table_key = (table or "").strip('"')
    columns: Iterable[Any] = []
    if isinstance(cfg, dict):
        columns = cfg.get(table_key) or cfg.get(table_key.upper()) or cfg.get("*") or []

    cols = _normalize_columns(columns)
    if not cols and isinstance(cfg, dict):
        # Try wildcard under quoted table name as well
        quoted = f'"{table_key}"'
        cols = _normalize_columns(cfg.get(quoted, []))

    if not cols and table_key == "Contract":
        cols = _normalize_columns(
            [
                "CONTRACT_SUBJECT",
                "CONTRACT_PURPOSE",
                "OWNER_DEPARTMENT",
                "DEPARTMENT_OUL",
                "CONTRACT_OWNER",
                "CONTRACT_STAKEHOLDER_1",
                "CONTRACT_STAKEHOLDER_2",
                "CONTRACT_STAKEHOLDER_3",
                "CONTRACT_STAKEHOLDER_4",
                "CONTRACT_STAKEHOLDER_5",
                "CONTRACT_STAKEHOLDER_6",
                "CONTRACT_STAKEHOLDER_7",
                "CONTRACT_STAKEHOLDER_8",
                "LEGAL_NAME_OF_THE_COMPANY",
                "ENTITY",
                "ENTITY_NO",
                "REQUEST_TYPE",
                "CONTRACT_STATUS",
                "REQUESTER",
                "CONTRACT_ID",
            ]
        )

    return cols


def get_short_token_allow(db: Any) -> List[str]:
    """Return allow-list of short tokens (<=2 chars) permitted in FTS."""

    allow = get_namespace_json(db, "DW_FTS_SHORT_TOKENS_ALLOW", default=["IT", "HR", "QA"])
    return [str(item).strip().upper() for item in allow if isinstance(item, str) and item.strip()]


# --- Backwards-compatibility shim ---
def get_setting(key, default=None, scope=None, as_type=None):
    """
    Backwards-compatible accessor used by legacy modules.
    Reads from the DB-backed settings (admin/settings/bulk) via get_namespace_json().
    - Ignores `scope` and `as_type` to maintain compatibility.
    - Returns the raw `value` already typed (bool/int/str/json), since our admin
      settings are stored with proper JSON types.
    """

    try:
        settings = Settings()
        return get_namespace_json(settings, key, default)
    except Exception:
        # Fail safe: never crash the app because of settings lookup
        return default


__all__ = [
    "get_settings",
    "get_namespace_json",
    "get_fts_columns",
    "get_short_token_allow",
    "get_setting",
]
