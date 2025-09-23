from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Tuple

import re

__all__ = ["NLIntent", "parse_intent", "build_sql"]


_NUM_WORDS = {
    "one": 1,
    "two": 2,
    "three": 3,
    "four": 4,
    "five": 5,
    "six": 6,
    "seven": 7,
    "eight": 8,
    "nine": 9,
    "ten": 10,
    "eleven": 11,
    "twelve": 12,
    "thirteen": 13,
    "fourteen": 14,
    "fifteen": 15,
    "sixteen": 16,
    "seventeen": 17,
    "eighteen": 18,
    "nineteen": 19,
    "twenty": 20,
    "thirty": 30,
    "forty": 40,
    "fifty": 50,
    "sixty": 60,
    "seventy": 70,
    "eighty": 80,
    "ninety": 90,
}


def _word_to_int(token: str) -> Optional[int]:
    token = token.strip().lower()
    if token.isdigit():
        return int(token)
    return _NUM_WORDS.get(token)


def _today() -> datetime:
    now = datetime.utcnow().replace(tzinfo=timezone.utc)
    return datetime(
        year=now.year,
        month=now.month,
        day=now.day,
        tzinfo=timezone.utc,
    )


def _shift_month_start(dt: datetime, delta_months: int) -> datetime:
    total_months = dt.year * 12 + (dt.month - 1) + delta_months
    year, month_index = divmod(total_months, 12)
    month = month_index + 1
    return dt.replace(year=year, month=month, day=1)


@dataclass
class NLIntent:
    has_time_window: bool = False
    date_column: Optional[str] = None
    explicit_start: Optional[datetime] = None
    explicit_end: Optional[datetime] = None
    agg: Optional[str] = None
    group_by: Optional[str] = None
    measure_sql: Optional[str] = None
    sort_by: Optional[str] = None
    sort_desc: Optional[bool] = None
    top_n: Optional[int] = None
    user_requested_top_n: Optional[bool] = None
    wants_all_columns: bool = False


def _last_month_window(today: datetime) -> Tuple[datetime, datetime]:
    start_of_month = today.replace(day=1)
    end_prev = start_of_month - timedelta(days=1)
    start_prev = end_prev.replace(day=1)
    start_prev = start_prev.replace(hour=0, minute=0, second=0, microsecond=0)
    end_prev = end_prev.replace(hour=0, minute=0, second=0, microsecond=0)
    return start_prev, end_prev


def _last_n_months_window(today: datetime, months: int) -> Tuple[datetime, datetime]:
    start_this_month = today.replace(day=1)
    end_prev = start_this_month - timedelta(days=1)
    start_prev = _shift_month_start(start_this_month, -months)
    return (
        start_prev.replace(hour=0, minute=0, second=0, microsecond=0),
        end_prev.replace(hour=0, minute=0, second=0, microsecond=0),
    )


def _last_quarter_window(today: datetime) -> Tuple[datetime, datetime]:
    quarter = (today.month - 1) // 3 + 1
    first_month_this_quarter = 3 * (quarter - 1) + 1
    start_this_quarter = today.replace(month=first_month_this_quarter, day=1)
    end_prev = start_this_quarter - timedelta(days=1)
    start_prev = _shift_month_start(start_this_quarter, -3)
    return (
        start_prev.replace(hour=0, minute=0, second=0, microsecond=0),
        end_prev.replace(hour=0, minute=0, second=0, microsecond=0),
    )


def parse_intent(text: str, default_date_col: str = "REQUEST_DATE") -> NLIntent:
    normalized = (text or "").strip()
    lowered = normalized.lower()
    today = _today()
    intent = NLIntent()

    if "(count)" in lowered or re.search(r"\bcount\b", lowered):
        intent.agg = "count"

    if "gross" in lowered:
        intent.measure_sql = (
            "NVL(CONTRACT_VALUE_NET_OF_VAT,0) + "
            "CASE WHEN NVL(VAT,0) BETWEEN 0 AND 1 "
            "THEN NVL(CONTRACT_VALUE_NET_OF_VAT,0) * NVL(VAT,0) "
            "ELSE NVL(VAT,0) END"
        )
    else:
        intent.measure_sql = "NVL(CONTRACT_VALUE_NET_OF_VAT,0)"

    if re.search(r"\b(per|by)\s+(owner\s+department|department)\b", lowered):
        intent.group_by = "OWNER_DEPARTMENT"
    elif re.search(r"\b(per|by)\s+entity\b", lowered):
        intent.group_by = "ENTITY_NO"
    elif re.search(r"\b(per|by)\s+owner\b", lowered):
        intent.group_by = "CONTRACT_OWNER"
    elif re.search(r"\b(per|by)\s+stakeholder\b", lowered) or "stakeholders" in lowered:
        intent.group_by = "CONTRACT_STAKEHOLDER_1"

    match = re.search(r"\btop\s+(\w+)", lowered)
    if match:
        candidate = _word_to_int(match.group(1))
        if candidate:
            intent.top_n = candidate
            intent.user_requested_top_n = True
            if intent.group_by and intent.measure_sql:
                intent.sort_by = intent.measure_sql
                intent.sort_desc = True

    if re.search(r"\b(expiring|expire|expiry|expires)\b", lowered):
        intent.date_column = "END_DATE"
    else:
        intent.date_column = default_date_col

    if re.search(r"\blast\s+month\b", lowered):
        intent.has_time_window = True
        intent.explicit_start, intent.explicit_end = _last_month_window(today)

    match = re.search(r"\blast\s+(\w+)\s+months?\b", lowered)
    if match:
        months = _word_to_int(match.group(1))
        if months:
            intent.has_time_window = True
            intent.explicit_start, intent.explicit_end = _last_n_months_window(today, months)

    if re.search(r"\blast\s+quarter\b", lowered):
        intent.has_time_window = True
        intent.explicit_start, intent.explicit_end = _last_quarter_window(today)

    match = re.search(r"\b(next|in)\s+(\w+)\s+days?\b", lowered)
    if match:
        days = _word_to_int(match.group(2))
        if days:
            intent.has_time_window = True
            intent.explicit_start = today
            intent.explicit_end = today + timedelta(days=days)

    if not intent.agg and not intent.group_by:
        intent.wants_all_columns = True

    return intent


def build_sql(intent: NLIntent, table: str = "Contract") -> Tuple[str, Dict[str, Any]]:
    if not table:
        return "", {}

    where_clause = ""
    binds: Dict[str, Any] = {}

    if (
        intent.has_time_window
        and intent.explicit_start
        and intent.explicit_end
        and intent.date_column
    ):
        where_clause = (
            f"WHERE {intent.date_column} BETWEEN :date_start AND :date_end"
        )
        binds["date_start"] = intent.explicit_start.date().isoformat()
        binds["date_end"] = intent.explicit_end.date().isoformat()

    limit_clause = ""
    if intent.top_n and intent.user_requested_top_n:
        limit_clause = "FETCH FIRST :top_n ROWS ONLY"
        binds["top_n"] = int(intent.top_n)

    if intent.agg == "count" and not intent.group_by:
        sql = f'SELECT COUNT(*) AS CNT\nFROM "{table}"'
        if where_clause:
            sql += f"\n{where_clause}"
        if limit_clause:
            sql += f"\n{limit_clause}"
        return sql, binds

    if intent.group_by:
        measure = intent.measure_sql or "COUNT(*)"
        alias = "MEASURE"
        if "NVL(CONTRACT_VALUE_NET_OF_VAT" in measure and "VAT" not in measure:
            alias = "NET_VALUE"
        elif "VAT" in measure:
            alias = "GROSS_VALUE"

        sql_lines = [
            "SELECT",
            f"  {intent.group_by} AS GROUP_KEY,",
            f"  SUM({measure}) AS {alias}",
            f'FROM "{table}"',
        ]
        if where_clause:
            sql_lines.append(where_clause)
        sql_lines.append(f"GROUP BY {intent.group_by}")
        if intent.sort_by:
            direction = "DESC" if intent.sort_desc else "ASC"
            sql_lines.append(f"ORDER BY {intent.sort_by} {direction}")
        elif intent.top_n:
            sql_lines.append(f"ORDER BY {alias} DESC")
        if limit_clause:
            sql_lines.append(limit_clause)
        return "\n".join(sql_lines), binds

    if intent.wants_all_columns:
        sql_lines = [f'SELECT * FROM "{table}"']
        if where_clause:
            sql_lines.append(where_clause)
        if intent.date_column and intent.has_time_window:
            sql_lines.append(f"ORDER BY {intent.date_column} ASC")
        if limit_clause:
            sql_lines.append(limit_clause)
        return "\n".join(sql_lines), binds

    return "", {}
