# English-only comments.
from typing import Dict, List

from apps.dw.settings import get_settings  # DB-backed (dw::common)


def resolve_eq_targets(column_token: str) -> List[str]:
    """
    Expand equality aliases like 'DEPARTMENT'/'DEPARTMENTS' and
    'STAKEHOLDER'/'STAKEHOLDERS' to their actual column lists using
    DW_EQ_ALIAS_COLUMNS. Fallback to the token itself if not aliased.
    """
    settings = get_settings() or {}
    aliases: Dict[str, List[str]] = settings.get("DW_EQ_ALIAS_COLUMNS", {})
    key = (column_token or "").strip().upper()
    if not key:
        return []
    return aliases.get(key, [key])
