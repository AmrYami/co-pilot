from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any, Dict, Optional, Tuple

try:  # precise month/quarter arithmetic if available
    from dateutil.relativedelta import relativedelta
except Exception:  # pragma: no cover - optional dependency
    relativedelta = None  # type: ignore[assignment]

try:  # natural language time expressions
    import dateparser
except Exception:  # pragma: no cover - optional dependency
    dateparser = None  # type: ignore[assignment]

try:  # text numbers (e.g., "ten")
    from word2number import w2n
except Exception:  # pragma: no cover - optional dependency
    w2n = None  # type: ignore[assignment]

DIMENSION_MAP = {
    "owner department": "OWNER_DEPARTMENT",
    "department": "OWNER_DEPARTMENT",
    "entity": "ENTITY_NO",
    "owner": "CONTRACT_OWNER",
    "stakeholder": "CONTRACT_STAKEHOLDER_1",
    "stakeholders": "CONTRACT_STAKEHOLDER_1",
}

WINDOW_HINTS = [
    (r"\blast\s+month\b", ("last_month", None)),
    (r"\blast\s+3\s+months?\b", ("last_3_months", None)),
    (r"\blast\s+quarter\b", ("last_quarter", None)),
    (r"\bnext\s+(\d+)\s+days\b", ("next_n_days", "END_DATE")),
    (r"\bexpir\w+\b", (None, "END_DATE")),
]


@dataclass
class DWIntent:
    agg: Optional[str] = None  # 'count' | 'sum' | None
    dimension: Optional[str] = None  # mapped DB column
    measure: str = "gross"  # 'gross'|'net'
    user_requested_top_n: bool = False
    top_n: Optional[int] = None
    date_column: Optional[str] = None  # REQUEST_DATE | END_DATE | START_DATE
    window_key: Optional[str] = None  # 'last_month' | 'last_3_months' | 'last_quarter' | 'next_n_days'
    wants_all_columns: bool = False
    window_param: Optional[int] = None  # e.g., number of days for next_n_days


def extract_intent(q: str) -> DWIntent:
    t = (q or "").strip().lower()
    intent = DWIntent()

    # count?
    if "count" in t or "(count)" in t:
        intent.agg = "count"

    # by/per <dimension>
    m = re.search(r"\b(?:by|per)\s+([a-z\s_]+)", t)
    if m:
        key = m.group(1).strip()
        for k, col in DIMENSION_MAP.items():
            if k in key:
                intent.dimension = col
                break

    # “top N …”
    m = re.search(r"\btop\s+(\d+)\b", t)
    if m:
        intent.user_requested_top_n = True
        intent.top_n = int(m.group(1))

    # gross vs net
    if "gross" in t:
        intent.measure = "gross"
    elif "net" in t:
        intent.measure = "net"

    # date hints
    for pat, (wkey, force_col) in WINDOW_HINTS:
        mm = re.search(pat, t)
        if mm:
            if wkey == "next_n_days":
                intent.window_key = "next_n_days"
                try:
                    intent.window_param = int(mm.group(1))
                except (ValueError, IndexError, TypeError):
                    intent.window_param = None
                intent.date_column = force_col or intent.date_column
            else:
                intent.window_key = wkey
            if force_col:
                intent.date_column = force_col
            break

    # default date column
    if intent.date_column is None:
        intent.date_column = "REQUEST_DATE"

    # wants all columns only if not aggregating and no dimension
    if intent.agg is None and intent.dimension is None:
        if any(w in t for w in ["list", "show", "contracts with", "all columns"]):
            intent.wants_all_columns = True

    return intent


_NUM_WORDS: Dict[str, int] = {
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
    "hundred": 100,
}


def _num_from_text(token: str) -> Optional[int]:
    token = (token or "").strip().lower()
    if not token:
        return None
    if token.isdigit():
        return int(token)
    if w2n:
        try:
            return w2n.word_to_num(token)
        except Exception:  # pragma: no cover - permissive
            pass
    return _NUM_WORDS.get(token)


@dataclass
class NLIntent:
    agg: Optional[str] = None
    measure_sql: Optional[str] = None
    group_by: Optional[str] = None
    sort_by: Optional[str] = None
    sort_desc: Optional[bool] = None
    top_n: Optional[int] = None
    user_requested_top_n: Optional[bool] = None
    wants_all_columns: bool = False
    has_time_window: bool = False
    date_column: Optional[str] = None
    explicit_dates: Optional[Dict[str, str]] = None
    notes: Dict[str, Any] = field(default_factory=dict)


_DET_DIM_SYNONYMS: Dict[str, str] = {
    r"\bowner department\b": "OWNER_DEPARTMENT",
    r"\bdepartment\b": "OWNER_DEPARTMENT",
    r"\bentity\b": "ENTITY_NO",
    r"\bstakeholder\b": "CONTRACT_STAKEHOLDER_1",
    r"\bowner\b": "CONTRACT_OWNER",
}


def _last_month_bounds(today: date) -> Tuple[date, date]:
    first_this = date(today.year, today.month, 1)
    last_month_end = first_this - timedelta(days=1)
    last_month_start = date(last_month_end.year, last_month_end.month, 1)
    return last_month_start, last_month_end


def _last_quarter_bounds(today: date) -> Tuple[date, date]:
    quarter = (today.month - 1) // 3 + 1
    first_this_q = date(today.year, 3 * (quarter - 1) + 1, 1)
    last_q_end = first_this_q - timedelta(days=1)
    last_q_start = date(last_q_end.year, 3 * ((last_q_end.month - 1) // 3) + 1, 1)
    return last_q_start, last_q_end


def _iso(d: date) -> str:
    return d.isoformat()


def parse_dw_intent(q: str, *, default_date_col: str = "REQUEST_DATE") -> NLIntent:
    text = (q or "").strip()
    lowered = text.lower()
    today = datetime.now().date()
    intent = NLIntent(notes={"q": q, "dateparser_available": bool(dateparser)})

    if "(count)" in lowered or re.search(r"\bcount\b", lowered):
        intent.agg = "count"

    if "gross" in lowered:
        intent.agg = intent.agg or "sum"
        intent.measure_sql = (
            "NVL(CONTRACT_VALUE_NET_OF_VAT,0) + "
            "CASE WHEN NVL(VAT,0) BETWEEN 0 AND 1 "
            "THEN NVL(CONTRACT_VALUE_NET_OF_VAT,0) * NVL(VAT,0) ELSE NVL(VAT,0) END"
        )
    elif any(key in lowered for key in ["contract value", "net value", "value"]):
        intent.agg = intent.agg or "sum"
        intent.measure_sql = "NVL(CONTRACT_VALUE_NET_OF_VAT,0)"

    match = re.search(r"\b(?:by|per)\s+([a-zA-Z_ ]+)", lowered)
    if match:
        dim_raw = match.group(1).strip()
        for pattern, column in _DET_DIM_SYNONYMS.items():
            if re.search(pattern, dim_raw):
                intent.group_by = column
                break
        if not intent.group_by and "stakeholder" in dim_raw:
            intent.group_by = "CONTRACT_STAKEHOLDER_1"

    match = re.search(r"\b(top|highest|bottom|lowest|least|smallest|cheapest|min)\s+([a-zA-Z0-9\-]+)", lowered)
    if match:
        number = _num_from_text(match.group(2))
        if number:
            intent.top_n = number
            intent.user_requested_top_n = True
            keyword = match.group(1)
            if keyword in {"top", "highest"}:
                intent.sort_desc = True
            elif keyword in {"bottom", "lowest", "least", "smallest", "cheapest", "min"}:
                intent.sort_desc = False

    match = re.search(r"\b(expir(?:e|ing)s?|due|ending)\s+in\s+([a-zA-Z0-9\-]+)\s+day", lowered)
    if match:
        number = _num_from_text(match.group(2)) or 30
        start = today
        end = today + timedelta(days=number)
        intent.has_time_window = True
        intent.date_column = "END_DATE"
        intent.explicit_dates = {"start": _iso(start), "end": _iso(end)}

    if not intent.has_time_window:
        match = re.search(
            r"\b(next|within)\s+([a-zA-Z0-9\-]+)\s+(day|days|week|weeks|month|months)\b",
            lowered,
        )
        if match:
            number = _num_from_text(match.group(2)) or 1
            unit = match.group(3)
            start = today
            if unit.startswith("day"):
                end = today + timedelta(days=number)
            elif unit.startswith("week"):
                end = today + timedelta(days=7 * number)
            else:
                if relativedelta:
                    end = today + relativedelta(months=+number)
                else:
                    end = today + timedelta(days=30 * number)
            intent.has_time_window = True
            intent.date_column = "END_DATE" if "expir" in lowered else default_date_col
            intent.explicit_dates = {"start": _iso(start), "end": _iso(end)}

    if not intent.has_time_window:
        if "last quarter" in lowered:
            start, end = _last_quarter_bounds(today)
            intent.has_time_window = True
            intent.date_column = default_date_col
            intent.explicit_dates = {"start": _iso(start), "end": _iso(end)}
        elif "last month" in lowered:
            start, end = _last_month_bounds(today)
            intent.has_time_window = True
            intent.date_column = default_date_col
            intent.explicit_dates = {"start": _iso(start), "end": _iso(end)}
        else:
            match = re.search(r"\blast\s+([a-zA-Z0-9\-]+)\s+months?\b", lowered)
            if match:
                number = _num_from_text(match.group(1)) or 1
                last_month_start, last_month_end = _last_month_bounds(today)
                if relativedelta:
                    start = date(last_month_start.year, last_month_start.month, 1) + relativedelta(
                        months=-(number - 1)
                    )
                else:
                    start = date(last_month_start.year, last_month_start.month, 1)
                end = last_month_end
                intent.has_time_window = True
                intent.date_column = default_date_col
                intent.explicit_dates = {"start": _iso(start), "end": _iso(end)}

    if intent.top_n and not intent.sort_by:
        if intent.measure_sql:
            intent.sort_by = intent.measure_sql
            if intent.sort_desc is None:
                intent.sort_desc = True

    if not intent.group_by and not intent.agg:
        intent.wants_all_columns = True

    if intent.has_time_window and not intent.date_column:
        intent.date_column = default_date_col

    return intent


def build_sql_for_intent(intent: NLIntent, *, table: str = "Contract") -> Tuple[str, Dict[str, Any]]:
    binds: Dict[str, Any] = {}
    where_clause = ""

    if intent.has_time_window and intent.explicit_dates and intent.date_column:
        binds["date_start"] = intent.explicit_dates["start"]
        binds["date_end"] = intent.explicit_dates["end"]
        where_clause = f"WHERE {intent.date_column} BETWEEN :date_start AND :date_end"

    raw_table = (table or "Contract").strip().strip('"')
    table_literal = f'"{raw_table}"'

    if intent.agg == "count" and not intent.group_by:
        sql = f"SELECT COUNT(*) AS CNT FROM {table_literal}"
        if where_clause:
            sql = f"{sql}\n{where_clause}"
        return sql, binds

    if intent.group_by and intent.measure_sql:
        lines = [
            "SELECT",
            f"  {intent.group_by} AS GROUP_KEY,",
            f"  SUM({intent.measure_sql}) AS MEASURE_VAL",
            f"FROM {table_literal}",
        ]
        if where_clause:
            lines.append(where_clause)
        lines.append(f"GROUP BY {intent.group_by}")
        if intent.sort_by:
            order_col = "MEASURE_VAL" if intent.sort_by == intent.measure_sql else intent.sort_by
            direction = "DESC" if intent.sort_desc or intent.sort_desc is None else "ASC"
            lines.append(f"ORDER BY {order_col} {direction}")
        if intent.top_n:
            binds["top_n"] = intent.top_n
            lines.append("FETCH FIRST :top_n ROWS ONLY")
        return "\n".join(lines), binds

    if intent.wants_all_columns:
        sql = f"SELECT * FROM {table_literal}"
        if where_clause:
            sql = f"{sql}\n{where_clause}"
        if intent.date_column:
            sql = f"{sql}\nORDER BY {intent.date_column} ASC"
        return sql, binds

    projection = ["CONTRACT_ID", "CONTRACT_OWNER"]
    if intent.date_column:
        projection.append(intent.date_column)
    sql = f"SELECT {', '.join(projection)} FROM {table_literal}"
    if where_clause:
        sql = f"{sql}\n{where_clause}"
    if intent.date_column:
        sql = f"{sql}\nORDER BY {intent.date_column} ASC"
    if intent.top_n:
        binds["top_n"] = intent.top_n
        sql = f"{sql}\nFETCH FIRST :top_n ROWS ONLY"
    return sql, binds
