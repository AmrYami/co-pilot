# English-only comments.
from typing import Dict, Iterable, List

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


def _dedupe(values: Iterable[str]) -> List[str]:
    seen: set[str] = set()
    result: List[str] = []
    for raw in values:
        text = str(raw or "").strip()
        if not text:
            continue
        key = text.upper()
        if key in seen:
            continue
        seen.add(key)
        result.append(text)
    return result


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
            return _dedupe(values)
        if isinstance(values, str) and values.strip():
            return [values.strip()]

    if key in {"DEPARTMENT", "DEPARTMENTS"}:
        explicit = _fetch(settings, "DW_EXPLICIT_FILTER_COLUMNS", []) or []
        columns = [str(c).strip() for c in explicit if str(c or "").strip()]
        expanded: List[str] = []
        seen: set[str] = set()
        for col in columns:
            upper = col.upper()
            if upper.startswith("DEPARTMENT_") and upper not in seen:
                expanded.append(upper)
                seen.add(upper)
        if any(col.upper() == "OWNER_DEPARTMENT" for col in columns):
            if "OWNER_DEPARTMENT" not in seen:
                expanded.append("OWNER_DEPARTMENT")
                seen.add("OWNER_DEPARTMENT")
        if not expanded:
            expanded.extend([f"DEPARTMENT_{i}" for i in range(1, 9)])
            expanded.append("OWNER_DEPARTMENT")
        return _dedupe(expanded) or [key]

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
