# English-only comments.
from typing import Dict, List

try:  # pragma: no cover - defensive fallback when settings backend is unavailable
    from apps.dw.settings import get_settings  # DB-backed (dw::common)
except ModuleNotFoundError:  # pragma: no cover
    def get_settings():
        return {}


def _fetch(settings: Dict | object, key: str, default=None):
    if isinstance(settings, dict):
        return settings.get(key, default)
    getter = getattr(settings, "get", None)
    if callable(getter):
        try:
            return getter(key)
        except TypeError:
            return getter(key, default)
    return default


def resolve_eq_targets(column_token: str) -> List[str]:
    """Expand equality aliases based on DW settings with smart fallbacks."""

    settings = get_settings() or {}
    key = (column_token or "").strip().upper()
    if not key:
        return []

    aliases = _fetch(settings, "DW_EQ_ALIAS_COLUMNS", {}) or {}
    if isinstance(aliases, dict) and key in aliases:
        values = aliases[key]
        if isinstance(values, (list, tuple, set)):
            return [str(v).strip() for v in values if str(v or "").strip()]
        if isinstance(values, str) and values.strip():
            return [values.strip()]

    if key in {"DEPARTMENT", "DEPARTMENTS"}:
        explicit = _fetch(settings, "DW_EXPLICIT_FILTER_COLUMNS", []) or []
        columns = [str(c).strip() for c in explicit if str(c or "").strip()]
        expanded = [c for c in columns if c.upper().startswith("DEPARTMENT_")]
        if any(c.upper() == "OWNER_DEPARTMENT" for c in columns):
            expanded.append("OWNER_DEPARTMENT")
        return expanded or [key]

    if key in {"STAKEHOLDER", "STAKEHOLDERS"}:
        slots_raw = _fetch(settings, "DW_STAKEHOLDER_SLOTS", 8)
        try:
            slots = int(slots_raw)
        except (TypeError, ValueError):
            slots = 8
        if slots < 1:
            slots = 1
        return [f"CONTRACT_STAKEHOLDER_{i}" for i in range(1, slots + 1)]

    return [key]
