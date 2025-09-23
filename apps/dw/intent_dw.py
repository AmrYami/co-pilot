from __future__ import annotations
import re
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Optional, Dict, Any, Tuple

EXPIRING_RE = re.compile(r'\b(expir(?:e|es|ing)|due)(?:\s+\w+){0,3}\s+\b(?:in|within)\s+(\d+)\s+day', re.I)
COUNT_RE    = re.compile(r'\(count\)|\bcount\b', re.I)


@dataclass
class DWIntent:
    agg: Optional[str] = None                 # 'count' | 'sum' | None
    has_time_window: bool = False
    date_column: Optional[str] = None         # 'END_DATE' | 'REQUEST_DATE' | 'START_DATE'
    explicit_dates: Optional[Dict[str, str]] = None  # {'start': 'YYYY-MM-DD', 'end': 'YYYY-MM-DD'}
    wants_all_columns: bool = False
    group_by: Optional[str] = None
    sort_by: Optional[str] = None
    sort_desc: Optional[bool] = None
    top_n: Optional[int] = None
    user_requested_top_n: Optional[bool] = None


def _iso(d: date) -> str:
    return d.isoformat()


def parse_intent_dw(q: str) -> DWIntent:
    """Very small, deterministic NLU for the DW app."""
    q = (q or "").strip()
    intent = DWIntent()
    if not q:
        return intent

    if COUNT_RE.search(q):
        intent.agg = "count"

    m = EXPIRING_RE.search(q)
    if m:
        days = int(m.group(2))
        today = date.today()
        start = today
        end = today + timedelta(days=days)
        intent.has_time_window = True
        intent.date_column = intent.date_column or "END_DATE"
        intent.explicit_dates = {"start": _iso(start), "end": _iso(end)}

    intent.wants_all_columns = intent.agg is None
    return intent


def build_sql_from_intent(intent: DWIntent, table: str = "Contract") -> Tuple[Optional[str], Dict[str, Any]]:
    """Returns (sql, binds) or (None, {}) if we shouldn't take the deterministic path."""
    if intent.agg == "count" and intent.has_time_window and intent.date_column and intent.explicit_dates:
        date_col = intent.date_column
        table_literal = (table or "Contract").strip()
        if not table_literal:
            table_literal = "Contract"
        if not table_literal.startswith('"') and "." not in table_literal:
            table_literal = f'"{table_literal}"'
        sql = (
            f'SELECT COUNT(*) AS CNT\n'
            f'FROM {table_literal}\n'
            f'WHERE {date_col} BETWEEN :date_start AND :date_end'
        )
        binds = {
            "date_start": intent.explicit_dates["start"],
            "date_end": intent.explicit_dates["end"],
        }
        return sql, binds

    return None, {}
