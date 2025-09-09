"""
Small helpers for safe-ish SQL text tweaks without a full parser.
Keep generic; FA specifics stay in apps/fa.
"""
from __future__ import annotations
import re
from typing import Optional, Tuple

_WHERE_RE = re.compile(r"(?is)\bwhere\b")

_SQL_START = re.compile(r"(?is)\b(SELECT|WITH|INSERT|UPDATE|DELETE)\b")


def extract_sql(text: str) -> Tuple[Optional[str], str]:
    """
    Try hard to pull a single SQL statement from LLM output.
    Returns (sql or None, how_extracted).
    """
    if not text:
        return None, "empty"

    # ```sql ... ```
    m = re.search(r"```sql\s*(.*?)```", text, re.DOTALL | re.IGNORECASE)
    if m:
        body = m.group(1).strip()
        m2 = _SQL_START.search(body)
        if m2:
            sql = body[m2.start():].strip()
            return sql, "fenced"

    # Inline SQL (first SELECT/WITH/… onward)
    m = _SQL_START.search(text)
    if m:
        body = text[m.start():].strip()
        # cut to last ';' if present
        last_semicolon = body.rfind(";")
        if last_semicolon != -1:
            body = body[: last_semicolon + 1]
        return body.strip(), "inline"

    return None, "not_found"

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
