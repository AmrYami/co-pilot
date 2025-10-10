from __future__ import annotations

from typing import Dict, List

from apps.dw.settings import get_settings


def resolve_eq_targets(column_token: str) -> List[str]:
    """
    Expand 'DEPARTMENT'/'STAKEHOLDER' (and plurals) using DW_EQ_ALIAS_COLUMNS.
    Fallback to the original token if no alias is defined.
    """
    settings = get_settings() or {}
    aliases: Dict[str, List[str]] = settings.get("DW_EQ_ALIAS_COLUMNS", {})
    key = (column_token or "").strip().upper()
    if not key:
        return []
    candidates = [key]
    if key.endswith("*"):
        candidates.insert(0, key.rstrip("*"))
    else:
        candidates.append(f"{key}*")
    for cand in candidates:
        cols = aliases.get(cand)
        if cols:
            return cols
    return [key]
