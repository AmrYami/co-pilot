"""
Small helpers for safe-ish SQL text tweaks without a full parser.
Keep generic; FA specifics stay in apps/fa.
"""
from __future__ import annotations
import re

_WHERE_RE = re.compile(r"(?is)\bwhere\b")

# Fenced code block: ```sql ... ```
_SQL_FENCE = re.compile(r"```(?:sql)?\s*(.*?)```", re.I | re.S)
# First SQL-ish token
_SQL_START = re.compile(r"(?is)\b(SELECT|WITH|EXPLAIN|SHOW)\b")


def extract_sql(text: str) -> str | None:
    """Pull a single SQL statement out of model output (markdown fences, chatter, etc.)."""
    if not text:
        return None
    body = text.strip()
    m = _SQL_FENCE.search(body)
    if m:
        body = m.group(1).strip()
    # drop leading 'sql:' labels etc.
    body = re.sub(r"^\s*sql\s*:\s*", "", body, flags=re.I).strip()
    m2 = _SQL_START.search(body)
    if not m2:
        return None
    sql = body[m2.start():]
    # stop at a trailing fence if present
    sql = sql.split("```", 1)[0].strip()
    # keep up to the last semicolon if multiple statements
    if ";" in sql:
        sql = sql[: sql.rfind(";") + 1]
    # strip stray backticks
    sql = sql.replace("`", "").strip()
    return sql or None


def looks_like_sql(text: str) -> bool:
    return bool(_SQL_START.search((text or "").strip()))

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
