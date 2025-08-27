"""
Small helpers for safe-ish SQL text tweaks without a full parser.
Keep generic; FA specifics stay in apps/fa.
"""
from __future__ import annotations
import re
from typing import Optional

_WHERE_RE = re.compile(r"(?is)\bwhere\b")

def _strip_semicolon(sql: str) -> str:
    return sql.rstrip().rstrip(";").rstrip()

def inject_between_date_filter(sql: str, fully_qualified_col: str, start_iso: str, end_iso: str) -> str:
    """
    Add a BETWEEN 'YYYY-MM-DD' AND 'YYYY-MM-DD' condition on the given column.
    If a WHERE exists -> append AND (...). Otherwise add WHERE (...).
    Leaves everything else intact. Returns modified SQL string.
    """
    base = _strip_semicolon(sql)
    cond = f"{fully_qualified_col} BETWEEN '{start_iso}' AND '{end_iso}'"
    if _WHERE_RE.search(base):
        return f"{base} AND {cond}"
    return f"{base} WHERE {cond}"
