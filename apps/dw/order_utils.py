"""Helpers for normalizing ORDER BY hints shared across DW endpoints."""
from __future__ import annotations

import re
from typing import Optional, Tuple

_ORDER_HINT_RE = re.compile(r"^\s*([A-Za-z0-9_]+)\s*(ASC|DESC)?\s*$", re.IGNORECASE)


def normalize_order_hint(
    sort_by: Optional[str], sort_desc: Optional[bool]
) -> Tuple[Optional[str], Optional[bool]]:
    """Extract the column and direction flag from a potentially ambiguous hint.

    ``sort_by`` may include an embedded ``ASC``/``DESC`` suffix. This helper strips
    the direction token (if present), uppercases the column for consistency and
    reconciles the ``sort_desc`` flag accordingly. The function returns the
    normalized column (or ``None`` when no column was provided) and the updated
    flag (which may remain ``None`` if no explicit direction was supplied).
    """

    if not sort_by:
        return None, sort_desc

    text = str(sort_by)
    match = _ORDER_HINT_RE.match(text)
    if match:
        column = match.group(1).upper()
        token = match.group(2)
        if token:
            sort_desc = token.upper() == "DESC"
        return column, sort_desc

    # Fallback: return trimmed uppercase column without inferring direction.
    return text.strip().upper(), sort_desc


__all__ = ["normalize_order_hint"]
