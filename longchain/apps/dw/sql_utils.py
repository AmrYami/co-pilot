"""SQL helper utilities for the lightweight DW app."""
from __future__ import annotations

import re

__all__ = ["enforce_single_order_by", "like_expr"]


_ORDER_BY_RE = re.compile(r"(?i)\bORDER\s+BY\b")


def enforce_single_order_by(sql: str) -> str:
    """Ensure that only the last ORDER BY clause is kept in *sql*."""

    if not sql:
        return sql

    parts = _ORDER_BY_RE.split(sql)
    if len(parts) <= 2:
        return sql

    head = "ORDER BY".join(parts[:-1]).rstrip()
    tail = parts[-1].strip()
    if not tail:
        return head
    return f"{head}\nORDER BY {tail}"


def like_expr(column: str, bind: str, *, oracle: bool = True) -> str:
    """Return a case-insensitive LIKE predicate for ``column``."""

    func = "NVL" if oracle else "COALESCE"
    return f"UPPER({func}({column},'')) LIKE UPPER(:{bind})"
