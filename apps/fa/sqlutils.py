from __future__ import annotations
import re

_LAST_MONTH_EQ = re.compile(
    r"""DATE_FORMAT\(\s*(?P<col>[a-zA-Z0-9_\.]+)\s*,\s*'%Y-%m'\s*\)\s*=\s*
        DATE_FORMAT\(\s*CURRENT_DATE\s*-\s*INTERVAL\s*1\s*MONTH\s*,\s*'%Y-%m'\s*\)""",
    re.IGNORECASE | re.VERBOSE,
)

_BETWEEN_LITERAL = re.compile(
    r"""(?P<col>[a-zA-Z0-9_\.]+)\s+BETWEEN\s+'(?P<start>\d{4}-\d{2}-\d{2})'\s+AND\s+'(?P<end>\d{4}-\d{2}-\d{2})'""",
    re.IGNORECASE | re.VERBOSE,
)

def widen_date_filter_mysql(sql: str, days: int = 90) -> str:
    """
    Heuristic widener for common 'last month' or literal BETWEEN patterns in MySQL.
    Rewrites to a rolling WINDOW:  col >= CURRENT_DATE - INTERVAL {days} DAY.
    Returns original SQL if no recognizable pattern found.
    """
    # 1) DATE_FORMAT(col, '%Y-%m') = DATE_FORMAT(CURRENT_DATE - INTERVAL 1 MONTH, '%Y-%m')
    def _repl_last_month(m: re.Match) -> str:
        col = m.group("col")
        return f"{col} >= CURRENT_DATE - INTERVAL {int(days)} DAY"

    new_sql = _LAST_MONTH_EQ.sub(_repl_last_month, sql)
    if new_sql != sql:
        return new_sql

    # 2) col BETWEEN 'YYYY-MM-DD' AND 'YYYY-MM-DD'  -> col >= CURRENT_DATE - INTERVAL {days} DAY
    def _repl_between(m: re.Match) -> str:
        col = m.group("col")
        return f"{col} >= CURRENT_DATE - INTERVAL {int(days)} DAY"

    newer_sql = _BETWEEN_LITERAL.sub(_repl_between, sql)
    return newer_sql
