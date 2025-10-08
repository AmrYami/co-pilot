# longchain/apps/dw/settings.py
# -*- coding: utf-8 -*-
"""
DW settings accessors.
All DW code should rely on these helpers to read runtime settings from DB.
Falls back safely when the central settings service is unavailable.
"""

from __future__ import annotations
from typing import Any, Dict, List, Optional
import os
import json
import logging

__all__ = [
    "get_namespace",
    "get_setting",
    "get_json_setting",
    "get_string_setting",
    "get_int_setting",
    "get_bool_setting",
    "get_json",
    "get_int",
    "get_bool",
    "clear_cache",
    "Settings",
    "DWSettings",
]

try:  # pragma: no cover - optional dependency during tests
    from sqlalchemy import create_engine, text
except Exception:  # pragma: no cover - allow running without SQLAlchemy
    create_engine = None  # type: ignore[assignment]

    def text(sql: str):  # type: ignore
        return sql

log = logging.getLogger(__name__)

# Local cache to avoid repeated round-trips (per-namespace payloads)
_SETTINGS_CACHE: Dict[str, Dict[str, Any]] = {}
_DEFAULT_NAMESPACE = "dw::common"
_ENV_NAMESPACE_KEY = "DW_NAMESPACE"


def get_namespace() -> str:
    """Return the active DW namespace."""

    namespace = os.getenv(_ENV_NAMESPACE_KEY, _DEFAULT_NAMESPACE)
    return namespace or _DEFAULT_NAMESPACE


_NAMESPACE = get_namespace()


class Settings:
    """Minimal settings facade used by the lightweight longchain DW app.

    The real DocuWare service talks to a central settings database. For the
    purposes of the longchain test harness we only need a predictable accessor
    that supports ``get``/``get_bool``/``get_int`` calls. Values can come from
    three sources (in order):

    1. Overrides passed at construction time (useful for tests).
    2. Environment variables (``{namespace}.{key}`` or plain ``key``).
    3. Provided default.

    The implementation intentionally keeps the surface tiny to avoid pulling in
    heavyweight database dependencies when the module is imported during unit
    tests.
    """

    def __init__(self, *, namespace: str = _NAMESPACE, values: Optional[Dict[str, Any]] = None) -> None:
        self.namespace = namespace
        self._values: Dict[str, Any] = dict(values or {})

    # ------------------------------------------------------------------
    def _env_key(self, key: str) -> str:
        return f"{self.namespace}.{key}" if self.namespace else key

    # ------------------------------------------------------------------
    def get(self, key: str, default: Any = None, *, scope: str = "namespace", namespace: Optional[str] = None) -> Any:
        if namespace and namespace != self.namespace:
            # naive multi-namespace support – construct a scoped helper on the fly
            scoped = Settings(namespace=namespace, values=self._values)
            return scoped.get(key, default, scope=scope)

        if scope not in {"namespace", None}:  # pragma: no cover - defensive
            return default

        if key in self._values:
            return self._values[key]

        env_override = os.getenv(self._env_key(key))
        if env_override is not None:
            return env_override

        env_plain = os.getenv(key)
        if env_plain is not None:
            return env_plain

        return default

    # ------------------------------------------------------------------
    def get_bool(self, key: str, default: Optional[bool] = None, **kwargs: Any) -> Optional[bool]:
        value = self.get(key, default=default, **kwargs)
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            lowered = value.strip().lower()
            if lowered in {"1", "true", "t", "yes", "y", "on"}:
                return True
            if lowered in {"0", "false", "f", "no", "n", "off"}:
                return False
        if value is None:
            return default
        return bool(value)

    # ------------------------------------------------------------------
    def get_int(self, key: str, default: Optional[int] = None, **kwargs: Any) -> Optional[int]:
        value = self.get(key, default=default, **kwargs)
        if value is None:
            return default
        if isinstance(value, int):
            return value
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    # ------------------------------------------------------------------
    def update(self, **items: Any) -> None:
        self._values.update(items)

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

def _get_namespace_cache(namespace: str) -> Dict[str, Any]:
    if namespace not in _SETTINGS_CACHE:
        _SETTINGS_CACHE[namespace] = {}
    return _SETTINGS_CACHE[namespace]


def _get_raw(key: str, default: Any = None, *, namespace: Optional[str] = None) -> Any:
    """
    Read a setting from the central settings service (if exposed),
    otherwise use cache/env/default.
    """

    ns = namespace or get_namespace()
    cache = _get_namespace_cache(ns)
    if key in cache:
        return cache[key]

    val: Any = None

    # 1) Preferred: central settings getter (DB-backed)
    if _GET_SETTING_FN:
        try:
            try:
                val = _GET_SETTING_FN(key, default=default, scope="namespace", namespace=ns)
            except TypeError:
                try:
                    val = _GET_SETTING_FN(key, default=default, namespace=ns)
                except TypeError:
                    try:
                        val = _GET_SETTING_FN(key, default=default, scope="namespace")
                    except TypeError:
                        val = _GET_SETTING_FN(key, default)
        except Exception as ex:
            log.warning("Settings getter failed for %s: %s", key, ex)

    # 2) Environment override (rarely used but handy in dev)
    if val is None:
        env_key = f"{ns}.{key}"
        if env_key in os.environ:
            val = os.environ.get(env_key)

    if val is None and key in os.environ:
        val = os.environ.get(key)

    # 3) Fallback to default (with guardrails for critical keys)
    if val is None:
        if key == "DW_FTS_ENGINE":
            val = "like" if default is None else default or "like"
        else:
            val = default

    cache[key] = val
    return val


def clear_cache(namespace: Optional[str] = None) -> None:
    if namespace is None:
        _SETTINGS_CACHE.clear()
    else:
        _SETTINGS_CACHE.pop(namespace, None)

# --- Typed readers ------------------------------------------------------------

def _resolve_scope_namespace(namespace: Optional[str], scope: Optional[str]) -> Optional[str]:
    if namespace:
        return namespace
    if scope in {None, "namespace"}:
        return get_namespace()
    return namespace


def get_setting(
    key: str,
    default: Any = None,
    *,
    namespace: Optional[str] = None,
    scope: Optional[str] = None,
) -> Any:
    ns = _resolve_scope_namespace(namespace, scope)
    return _get_raw(key, default, namespace=ns)


def get_bool_setting(
    key: str,
    default: bool = False,
    *,
    namespace: Optional[str] = None,
    scope: Optional[str] = None,
) -> bool:
    ns = _resolve_scope_namespace(namespace, scope)
    val = _get_raw(key, default, namespace=ns)
    if isinstance(val, bool):
        return val
    if isinstance(val, str):
        return val.strip().lower() in {"1", "true", "yes", "y", "on"}
    return bool(val)


def get_int_setting(
    key: str,
    default: int = 0,
    *,
    namespace: Optional[str] = None,
    scope: Optional[str] = None,
) -> int:
    ns = _resolve_scope_namespace(namespace, scope)
    val = _get_raw(key, default, namespace=ns)
    try:
        return int(val)
    except Exception:
        return default


def get_json_setting(
    key: str,
    default: Any = None,
    *,
    namespace: Optional[str] = None,
    scope: Optional[str] = None,
) -> Any:
    ns = _resolve_scope_namespace(namespace, scope)
    val = _get_raw(key, default, namespace=ns)
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


def get_string_setting(
    key: str,
    default: str = "",
    *,
    namespace: Optional[str] = None,
    scope: Optional[str] = None,
) -> str:
    ns = _resolve_scope_namespace(namespace, scope)
    val = _get_raw(key, default, namespace=ns)
    if val is None:
        return default
    return str(val)


# Backwards compatibility helpers ------------------------------------------------

def get_bool(key: str, default: bool = False, **kwargs: Any) -> bool:
    return get_bool_setting(key, default=default, **kwargs)


def get_int(key: str, default: int = 0, **kwargs: Any) -> int:
    return get_int_setting(key, default=default, **kwargs)


def get_json(key: str, default: Any = None, **kwargs: Any) -> Any:
    return get_json_setting(key, default=default, **kwargs)

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


class DWSettings:
    """Lightweight DB-backed settings facade for DW helpers."""

    def __init__(self, url: Optional[str] = None) -> None:
        self.mem_url = url or os.getenv("MEMORY_DB_URL")
        self._cache: Dict[str, Any] = {}
        self._engine = create_engine(self.mem_url) if (self.mem_url and create_engine) else None
        self.namespace = get_namespace()

    # ------------------------------------------------------------------
    def _fetch_all(self) -> Dict[str, Any]:
        if self._cache:
            return self._cache
        if not self._engine:
            return {}
        try:
            with self._engine.begin() as conn:
                rows = (
                    conn.execute(
                        text(
                            "SELECT key, value, value_type FROM settings WHERE namespace=:ns"
                        ),
                        {"ns": self.namespace},
                    )
                    .mappings()
                    .all()
                )
        except Exception:
            return {}

        data: Dict[str, Any] = {}
        for row in rows:
            val: Any = row.get("value")
            vtype = row.get("value_type")
            if vtype == "json" and isinstance(val, str):
                try:
                    val = json.loads(val)
                except Exception:
                    pass
            data[str(row.get("key"))] = val
        self._cache = data
        return self._cache

    # ------------------------------------------------------------------
    def get(self, key: str, default: Any = None) -> Any:
        return self._fetch_all().get(key, default)

    # ------------------------------------------------------------------
    def fts_engine(self) -> str:
        value = (self.get("DW_FTS_ENGINE", "like") or "like").strip().lower()
        if value not in {"like", "oracle-text"}:
            value = "like"
        return value

    # ------------------------------------------------------------------
    def fts_columns(self, table: str = "Contract") -> List[str]:
        conf = self.get("DW_FTS_COLUMNS", {}) or {}
        if not isinstance(conf, dict):
            return []
        if table in conf and isinstance(conf[table], list):
            return list(conf[table])
        if "*" in conf and isinstance(conf["*"], list):
            return list(conf["*"])
        return []

    # ------------------------------------------------------------------
    def explicit_eq_columns(self) -> List[str]:
        data = self.get("DW_EXPLICIT_FILTER_COLUMNS", []) or []
        if not isinstance(data, list):
            return []
        return [str(item) for item in data if str(item).strip()]

    # ------------------------------------------------------------------
    def enum_synonyms(self) -> Dict[str, Any]:
        mapping = self.get("DW_ENUM_SYNONYMS", {}) or {}
        return mapping if isinstance(mapping, dict) else {}
