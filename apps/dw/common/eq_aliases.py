"""Equality alias resolution utilities for DocuWare Contract planner."""
from __future__ import annotations

from functools import lru_cache
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional

try:  # pragma: no cover - optional import available in production
    from apps.mem.kv import get_settings_for_namespace  # type: ignore
except Exception:  # pragma: no cover - fall back to lazy loader
    get_settings_for_namespace = None  # type: ignore

try:  # pragma: no cover - imported lazily in tests
    from apps.dw.settings import get_setting  # type: ignore
except Exception:  # pragma: no cover - optional fallback
    get_setting = None  # type: ignore

_SETTING_KEY = "DW_EQ_ALIAS_COLUMNS"
_NAMESPACE = "dw::common"


def _normalize_key(raw: str) -> str:
    cleaned = (raw or "").strip().strip('"')
    if not cleaned:
        return ""
    return cleaned.replace("-", "_").replace(" ", "_").upper()


def _normalize_columns(values: Iterable[Any]) -> List[str]:
    seen: set[str] = set()
    normalized: List[str] = []
    for value in values or []:
        if not isinstance(value, str):
            continue
        text = value.strip()
        if not text:
            continue
        key = _normalize_key(text)
        if not key:
            continue
        if key in seen:
            continue
        seen.add(key)
        normalized.append(text.strip())
    return normalized


def _extract_mapping(settings: Any | None) -> Mapping[str, Any] | None:
    if settings is None:
        return None
    if isinstance(settings, Mapping):
        return settings
    getter = getattr(settings, "get", None)
    if callable(getter):
        try:
            value = getter(_SETTING_KEY)
        except TypeError:
            value = getter(_SETTING_KEY, None)
        if isinstance(value, Mapping):
            return {_SETTING_KEY: value}
    getter_json = getattr(settings, "get_json", None)
    if callable(getter_json):
        try:
            value = getter_json(_SETTING_KEY, None)
        except TypeError:
            value = getter_json(_SETTING_KEY)
        if isinstance(value, Mapping):
            return {_SETTING_KEY: value}
    to_dict = getattr(settings, "to_dict", None)
    if callable(to_dict):
        try:
            value = to_dict()
            if isinstance(value, Mapping):
                return value
        except Exception:  # pragma: no cover - defensive
            return None
    data = getattr(settings, "__dict__", None)
    if isinstance(data, Mapping):
        return data
    return None


def _coerce_alias_map(source: Any | None) -> Dict[str, List[str]]:
    mapping: Dict[str, List[str]] = {}
    if isinstance(source, Mapping) and _SETTING_KEY in source:
        raw = source.get(_SETTING_KEY)
    else:
        raw = source
    if isinstance(raw, Mapping):
        items = raw.items()
    elif isinstance(source, Mapping):
        items = source.items()
    else:
        items = []
    for key, value in items:
        if not isinstance(key, str):
            continue
        columns = _normalize_columns(value if isinstance(value, Iterable) else [])
        if not columns:
            continue
        norm_key = _normalize_key(key)
        if not norm_key:
            continue
        mapping[norm_key] = columns
        # Allow both starred and non-starred variants for compatibility with legacy tokens.
        if norm_key.endswith("*"):
            mapping.setdefault(norm_key.rstrip("*"), columns)
        else:
            mapping.setdefault(f"{norm_key}*", columns)
    return mapping


@lru_cache(maxsize=4)
def _load_global_alias_map() -> Dict[str, List[str]]:
    if get_settings_for_namespace is not None:  # pragma: no cover - fallback for tests
        try:
            settings = get_settings_for_namespace(_NAMESPACE)
        except Exception:  # pragma: no cover - settings backend issues
            settings = None
        if isinstance(settings, MutableMapping):
            mapped = _coerce_alias_map(settings)
            if mapped:
                return mapped

    if get_setting is not None:  # pragma: no cover - loaded dynamically
        try:
            raw = get_setting(_SETTING_KEY, default={})
        except Exception:
            raw = {}
        mapped = _coerce_alias_map({_SETTING_KEY: raw})
        if mapped:
            return mapped

    return {}


def get_eq_alias_map(settings: Any | None = None) -> Dict[str, List[str]]:
    """Return the equality alias mapping using the provided settings if available."""

    if settings is not None:
        candidate = _extract_mapping(settings)
        if candidate:
            mapped = _coerce_alias_map(candidate)
            if mapped:
                return mapped
    return dict(_load_global_alias_map())


def resolve_eq_targets(
    column_token: str,
    *,
    settings: Any | None = None,
    mapping: Optional[Dict[str, List[str]]] = None,
) -> List[str]:
    """Expand ``column_token`` to its target columns according to alias settings."""

    if not column_token:
        return []
    if mapping is None:
        mapping = get_eq_alias_map(settings)
    key = _normalize_key(column_token)
    if not key:
        return [column_token]
    targets = mapping.get(key)
    if targets:
        return list(targets)
    return [column_token]


__all__ = ["get_eq_alias_map", "resolve_eq_targets"]
