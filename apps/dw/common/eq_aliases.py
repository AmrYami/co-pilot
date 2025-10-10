# English-only comments.
from typing import Dict, List

try:  # pragma: no cover - defensive fallback when settings backend is unavailable
    from apps.dw.settings import get_settings  # DB-backed (dw::common)
except ModuleNotFoundError:  # pragma: no cover
    def get_settings():
        return {}


def resolve_eq_targets(column_token: str) -> List[str]:
    """
    Expand equality aliases like 'DEPARTMENT'/'DEPARTMENTS' and
    'STAKEHOLDER'/'STAKEHOLDERS' to their actual column lists using
    DW_EQ_ALIAS_COLUMNS. Fallback to the token itself if not aliased.
    """
    settings = get_settings() or {}
    key = (column_token or "").strip().upper()
    if not key:
        return []

    aliases_map: Dict[str, List[str]] = {}
    raw_aliases = None
    if isinstance(settings, dict):
        raw_aliases = settings.get("DW_EQ_ALIAS_COLUMNS")
    else:
        getter = getattr(settings, "get", None)
        if callable(getter):
            try:
                raw_aliases = getter("DW_EQ_ALIAS_COLUMNS")
            except TypeError:
                raw_aliases = getter("DW_EQ_ALIAS_COLUMNS", None)
            except Exception:  # pragma: no cover - defensive
                raw_aliases = None

    if isinstance(raw_aliases, dict):
        for alias_key, targets in raw_aliases.items():
            normalized_key = str(alias_key or "").strip().upper()
            if not normalized_key:
                continue
            if isinstance(targets, (list, tuple, set)):
                aliases_map[normalized_key] = [
                    str(target).strip() for target in targets if str(target or "").strip()
                ]
            elif isinstance(targets, str) and targets.strip():
                aliases_map[normalized_key] = [targets.strip()]

    if key in aliases_map:
        return aliases_map[key] or [key]

    if key in {"DEPARTMENT", "DEPARTMENTS"}:
        columns_setting = None
        if isinstance(settings, dict):
            columns_setting = settings.get("DW_EXPLICIT_FILTER_COLUMNS")
        else:
            getter = getattr(settings, "get", None)
            if callable(getter):
                try:
                    columns_setting = getter("DW_EXPLICIT_FILTER_COLUMNS")
                except TypeError:
                    columns_setting = getter("DW_EXPLICIT_FILTER_COLUMNS", None)
                except Exception:  # pragma: no cover - defensive
                    columns_setting = None
        columns: List[str] = []
        if isinstance(columns_setting, (list, tuple, set)):
            for value in columns_setting:
                text = str(value or "").strip()
                if text:
                    columns.append(text)
        owner_present = any(col.upper() == "OWNER_DEPARTMENT" for col in columns)
        expanded = [col for col in columns if col.upper().startswith("DEPARTMENT_")]
        if owner_present and "OWNER_DEPARTMENT" not in expanded:
            expanded.append("OWNER_DEPARTMENT")
        return expanded or [key]

    if key in {"STAKEHOLDER", "STAKEHOLDERS"}:
        slots_raw = None
        if isinstance(settings, dict):
            slots_raw = settings.get("DW_STAKEHOLDER_SLOTS")
        else:
            getter = getattr(settings, "get", None)
            if callable(getter):
                try:
                    slots_raw = getter("DW_STAKEHOLDER_SLOTS")
                except TypeError:
                    slots_raw = getter("DW_STAKEHOLDER_SLOTS", None)
                except Exception:  # pragma: no cover - defensive
                    slots_raw = None
        try:
            slots = int(slots_raw) if slots_raw is not None else 8
        except (TypeError, ValueError):
            slots = 8
        if slots < 1:
            slots = 1
        return [f"CONTRACT_STAKEHOLDER_{i}" for i in range(1, slots + 1)]

    return [key]
