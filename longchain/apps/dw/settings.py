# longchain/apps/dw/settings.py
# -*- coding: utf-8 -*-
"""
DW settings accessors.
All DW code should rely on these helpers to read runtime settings from DB.
Falls back safely when the central settings service is unavailable.
"""

from __future__ import annotations
from typing import Any, Dict, List
import os
import json
import logging

log = logging.getLogger(__name__)

# Local cache to avoid repeated round-trips
_SETTINGS_CACHE: Dict[str, Any] = {}
_NAMESPACE = "dw::common"

# --- Internal helpers ---------------------------------------------------------

def _try_import_settings_getter():
    """
    Try to import a project-level settings getter. We attempt multiple locations
    to match the current codebase structure without introducing hard dependencies.
    Return a callable (key: str, default: Any) -> Any or None if not found.
    """
    candidates = [
        # Prefer common/project-wide settings services if they exist
        ("apps.common.settings", "get_setting"),
        ("longchain.apps.common.settings", "get_setting"),
        ("apps.core.settings", "get_setting"),
        ("longchain.apps.core.settings", "get_setting"),
        # Generic fallback (if a repo exposes a module-level getter)
        ("settings", "get_setting"),
    ]
    for mod_name, func_name in candidates:
        try:
            mod = __import__(mod_name, fromlist=[func_name])
            fn = getattr(mod, func_name, None)
            if callable(fn):
                return fn
        except Exception:
            continue
    return None

_GET_SETTING_FN = _try_import_settings_getter()

def _get_raw(key: str, default: Any = None) -> Any:
    """
    Read a setting from the central settings service (if exposed),
    otherwise use cache/env/default.
    """
    cache_key = f"{_NAMESPACE}:{key}"
    if cache_key in _SETTINGS_CACHE:
        return _SETTINGS_CACHE[cache_key]

    val = None

    # 1) Preferred: central settings getter (DB-backed)
    if _GET_SETTING_FN:
        try:
            # Many central getters accept (key, default=None, scope=None/namespace)
            # We try to pass namespace via kwargs; fallback to positional if needed.
            try:
                val = _GET_SETTING_FN(key, default=default, scope="namespace")
            except TypeError:
                # Older signature
                val = _GET_SETTING_FN(key, default)
        except Exception as ex:
            log.warning("Settings getter failed for %s: %s", key, ex)

    # 2) Environment override (rarely used but handy in dev)
    if val is None:
        env_key = f"{_NAMESPACE}.{key}"
        if env_key in os.environ:
            val = os.environ.get(env_key)

    # 3) Fallback to default
    if val is None:
        val = default

    _SETTINGS_CACHE[cache_key] = val
    return val

def clear_cache():
    _SETTINGS_CACHE.clear()

# --- Typed readers ------------------------------------------------------------

def get_setting(key: str, default: Any = None) -> Any:
    return _get_raw(key, default)

def get_bool(key: str, default: bool = False) -> bool:
    val = _get_raw(key, default)
    if isinstance(val, bool):
        return val
    if isinstance(val, str):
        return val.strip().lower() in {"1", "true", "yes", "y", "on"}
    return bool(val)

def get_int(key: str, default: int = 0) -> int:
    val = _get_raw(key, default)
    try:
        return int(val)
    except Exception:
        return default

def get_json(key: str, default: Any = None) -> Any:
    val = _get_raw(key, default)
    if val is None:
        return default
    if isinstance(val, (dict, list)):
        return val
    if isinstance(val, str):
        try:
            return json.loads(val)
        except Exception:
            # Sometimes admin/settings/bulk already stores JSON as real JSON
            # but the underlying getter returns stringified JSON. We try best-effort.
            pass
    return val

# --- DW-specific helpers ------------------------------------------------------

def get_contract_table(default: str = "Contract") -> str:
    return str(get_setting("DW_CONTRACT_TABLE", default) or default)

def get_date_column(default: str = "REQUEST_DATE") -> str:
    return str(get_setting("DW_DATE_COLUMN", default) or default)

def get_fts_engine(default: str = "like") -> str:
    """
    Returns 'like' or 'oracle-text'.
    If the setting is missing or invalid, defaults to 'like'.
    """
    engine = str(get_setting("DW_FTS_ENGINE", default) or default).strip().lower()
    if engine not in {"like", "oracle-text"}:
        engine = "like"
    return engine

def get_fts_columns(table: str = "Contract") -> List[str]:
    """
    Combines DW_FTS_COLUMNS[table] + DW_FTS_COLUMNS['*'] if present.
    """
    conf = get_json("DW_FTS_COLUMNS", {}) or {}
    cols: List[str] = []
    # Strictly prefer table-specific
    if isinstance(conf, dict):
        if table in conf and isinstance(conf[table], list):
            cols.extend(conf[table])
        # Merge wildcard defaults
        if "*" in conf and isinstance(conf["*"], list):
            # Avoid duplicates while preserving order
            for c in conf["*"]:
                if c not in cols:
                    cols.append(c)
    # Final safety: unique + preserve order
    seen = set()
    uniq = []
    for c in cols:
        if c not in seen:
            seen.add(c)
            uniq.append(c)
    return uniq

def get_explicit_filter_columns() -> List[str]:
    """
    Columns allowed for equality filters (COLUMN = VALUE) — loaded from DB.
    """
    cols = get_json("DW_EXPLICIT_FILTER_COLUMNS", []) or []
    # normalize and unique
    norm = []
    seen = set()
    for c in cols:
        s = str(c).strip()
        if s and s not in seen:
            seen.add(s)
            norm.append(s)
    return norm

def get_enum_synonyms() -> Dict[str, Any]:
    """
    Synonyms map for enumerations, e.g. 'Contract.REQUEST_TYPE': {...}
    """
    m = get_json("DW_ENUM_SYNONYMS", {}) or {}
    if not isinstance(m, dict):
        return {}
    return m
