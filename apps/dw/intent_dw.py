from __future__ import annotations

import os
import re
from dataclasses import asdict, dataclass, field
from datetime import date, timedelta
from typing import Any, Dict, Optional, Tuple


DEFAULT_DATE_COL = os.getenv("DW_DATE_COLUMN", "REQUEST_DATE").strip() or "REQUEST_DATE"


_NUM_WORDS = {
    "ten": 10,
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


_DIMENSION_SYNONYMS = {
    "owner department": "OWNER_DEPARTMENT",
    "department": "OWNER_DEPARTMENT",
    "entity": "ENTITY_NO",
    "owner": "CONTRACT_OWNER",
    "stakeholder": "CONTRACT_STAKEHOLDER_1",
}


_RE_TOP = re.compile(r"\btop\s+(\d+|\w+)\b", re.I)
_RE_COUNT = re.compile(r"\bcount\b|\(count\)", re.I)
_RE_GROSS = re.compile(r"\bgross\b", re.I)
_RE_NET = re.compile(r"\bnet\b|\bcontract\s+value\b", re.I)
_RE_EXPIRING = re.compile(r"\bexpir\w*\b", re.I)
_RE_NEXT_N_DAYS = re.compile(r"\bnext\s+(\d+|\w+)\s+days\b", re.I)
_RE_LAST_MONTH = re.compile(r"\blast\s+month\b", re.I)
_RE_LAST_N_MONTHS = re.compile(r"\blast\s+(\d+)\s+months?\b", re.I)
_RE_LAST_QUARTER = re.compile(r"\blast\s+quarter\b", re.I)
_RE_YEAR_IN = re.compile(r"\bin\s+(20\d{2})\b", re.I)
_RE_REQUEST_TYPE = re.compile(r"\brequest[_\s]?type\b", re.I)


_VALUE_NET = "NVL(CONTRACT_VALUE_NET_OF_VAT,0)"
_VALUE_GROSS = (
    "NVL(CONTRACT_VALUE_NET_OF_VAT,0) + "
    "CASE WHEN NVL(VAT,0) BETWEEN 0 AND 1 "
    "THEN NVL(CONTRACT_VALUE_NET_OF_VAT,0) * NVL(VAT,0) ELSE NVL(VAT,0) END"
)


@dataclass
class DWIntent:
    has_time_window: Optional[bool] = None
    date_column: Optional[str] = None
    explicit_dates: Optional[Dict[str, str]] = None
    agg: Optional[str] = None
    wants_all_columns: Optional[bool] = None
    group_by: Optional[str] = None
    sort_by: Optional[str] = None
    sort_desc: Optional[bool] = None
    top_n: Optional[int] = None
    user_requested_top_n: Optional[bool] = None
    measure_sql: Optional[str] = None
    notes: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _iso(d: date) -> str:
    return d.isoformat()


def _last_month(today: date) -> Dict[str, str]:
    first_this_month = date(today.year, today.month, 1)
    last_day_prev = first_this_month - timedelta(days=1)
    start_prev = date(last_day_prev.year, last_day_prev.month, 1)
    return {"start": _iso(start_prev), "end": _iso(last_day_prev)}


def _last_n_months(today: date, n: int) -> Dict[str, str]:
    n = max(1, int(n))
    window = _last_month(today)
    start = date.fromisoformat(window["start"])
    for _ in range(max(0, n - 1)):
        start = (start - timedelta(days=1)).replace(day=1)
    return {"start": _iso(start), "end": window["end"]}


def _last_quarter(today: date) -> Dict[str, str]:
    quarter = (today.month - 1) // 3 + 1
    first_this_q = date(today.year, 3 * (quarter - 1) + 1, 1)
    last_q_end = first_this_q - timedelta(days=1)
    first_last_q = date(last_q_end.year, 3 * ((last_q_end.month - 1) // 3) + 1, 1)
    return {"start": _iso(first_last_q), "end": _iso(last_q_end)}


def _next_n_days(today: date, n: int) -> Dict[str, str]:
    n = max(0, int(n))
    return {"start": _iso(today), "end": _iso(today + timedelta(days=n))}


def _word_or_digit_to_int(value: str) -> Optional[int]:
    if not value:
        return None
    lowered = value.lower()
    if lowered.isdigit():
        try:
            return int(lowered)
        except ValueError:
            return None
    return _NUM_WORDS.get(lowered)


def _detect_group_by(text: str) -> Optional[str]:
    lowered = text.lower()
    for phrase, column in _DIMENSION_SYNONYMS.items():
        if re.search(rf"\b(?:by|per)\s+{re.escape(phrase)}\b", lowered):
            return column
    return None


def parse_intent_dw(q: str, *, today: Optional[date] = None) -> DWIntent:
    today = today or date.today()
    text = (q or "").strip()

    intent = DWIntent(
        has_time_window=None,
        date_column=None,
        explicit_dates=None,
        agg=None,
        wants_all_columns=True,
        group_by=None,
        sort_by=None,
        sort_desc=None,
        top_n=None,
        user_requested_top_n=None,
        measure_sql=None,
        notes={"q": q},
    )

    if not text:
        intent.date_column = DEFAULT_DATE_COL
        intent.has_time_window = False
        return intent

    if _RE_COUNT.search(text):
        intent.agg = "count"
        intent.wants_all_columns = False

    group_by = _detect_group_by(text)
    if group_by:
        intent.group_by = group_by
        intent.wants_all_columns = False

    match_top = _RE_TOP.search(text)
    if match_top:
        maybe_n = _word_or_digit_to_int(match_top.group(1))
        if maybe_n:
            intent.top_n = maybe_n
            intent.user_requested_top_n = True

    if _RE_GROSS.search(text):
        intent.measure_sql = _VALUE_GROSS
    elif _RE_NET.search(text) or intent.group_by or intent.top_n:
        intent.measure_sql = _VALUE_NET

    # Date hints
    if _RE_EXPIRING.search(text):
        intent.date_column = "END_DATE"
        match_days = _RE_NEXT_N_DAYS.search(text)
        days = _word_or_digit_to_int(match_days.group(1)) if match_days else None
        if days is None:
            days = 30
        intent.explicit_dates = _next_n_days(today, days)
        intent.has_time_window = True

    if intent.date_column != "END_DATE" and _RE_REQUEST_TYPE.search(text):
        intent.date_column = "REQUEST_DATE"

    if intent.explicit_dates is None:
        if _RE_LAST_MONTH.search(text):
            intent.explicit_dates = _last_month(today)
            intent.has_time_window = True
        else:
            match_last_n = _RE_LAST_N_MONTHS.search(text)
            if match_last_n:
                intent.explicit_dates = _last_n_months(today, int(match_last_n.group(1)))
                intent.has_time_window = True
            elif _RE_LAST_QUARTER.search(text):
                intent.explicit_dates = _last_quarter(today)
                intent.has_time_window = True

    if intent.explicit_dates is None:
        match_next = _RE_NEXT_N_DAYS.search(text)
        if match_next:
            days = _word_or_digit_to_int(match_next.group(1))
            if days is not None:
                intent.explicit_dates = _next_n_days(today, days)
                intent.has_time_window = True

    if intent.explicit_dates is None:
        match_year = _RE_YEAR_IN.search(text)
        if match_year:
            year = int(match_year.group(1))
            intent.explicit_dates = {
                "start": _iso(date(year, 1, 1)),
                "end": _iso(date(year, 12, 31)),
            }
            intent.has_time_window = True
            if intent.date_column in (None, DEFAULT_DATE_COL):
                intent.date_column = "REQUEST_DATE"

    if intent.date_column is None:
        intent.date_column = DEFAULT_DATE_COL

    if intent.top_n and not intent.sort_by:
        if intent.measure_sql:
            intent.sort_by = "MEASURE"
            intent.sort_desc = True
        else:
            intent.sort_by = intent.date_column
            intent.sort_desc = True

    if intent.has_time_window is None:
        intent.has_time_window = bool(intent.explicit_dates)

    return intent


def _table_literal(table: str) -> str:
    table = (table or "Contract").strip()
    if not table:
        table = "Contract"
    if table.startswith('"') or "." in table:
        return table
    return f'"{table.strip("\"")}"'


def build_sql_from_intent(intent: DWIntent | Dict[str, Any], table: str = "Contract") -> Tuple[Optional[str], Dict[str, Any]]:
    if intent is None:
        return None, {}

    if isinstance(intent, DWIntent):
        intent_data = intent.to_dict()
    else:
        intent_data = dict(intent)

    date_col = (intent_data.get("date_column") or DEFAULT_DATE_COL or "REQUEST_DATE").strip() or "REQUEST_DATE"
    date_col = date_col.upper()
    explicit_dates = intent_data.get("explicit_dates") or {}
    start = explicit_dates.get("start")
    end = explicit_dates.get("end")
    has_window = bool(start and end)

    has_time_window_flag = intent_data.get("has_time_window")
    if has_time_window_flag is True and not has_window:
        has_window = False

    binds: Dict[str, Any] = {}
    where_clause = ""
    if has_window:
        binds["date_start"] = start
        binds["date_end"] = end
        where_clause = f"WHERE {date_col} BETWEEN :date_start AND :date_end"

    notes = intent_data.get("notes") or {}
    question_text = str(notes.get("q") or "")
    if os.getenv("DW_REQUIRE_WINDOW_FOR_EXPIRE", "1").lower() in {"1", "true", "yes"}:
        if "expir" in question_text.lower() and not has_window:
            raise ValueError("expiring_query_missing_window")

    group_by = intent_data.get("group_by")
    agg = (intent_data.get("agg") or "").strip().lower() or None
    wants_all = intent_data.get("wants_all_columns")
    top_n = intent_data.get("top_n")
    if isinstance(top_n, str) and top_n.isdigit():
        top_n = int(top_n)
    user_requested_top_n = bool(intent_data.get("user_requested_top_n"))
    sort_by = intent_data.get("sort_by")
    sort_desc = bool(intent_data.get("sort_desc")) if intent_data.get("sort_desc") is not None else None
    measure_sql = intent_data.get("measure_sql") or None

    table_literal = _table_literal(table)

    if agg == "count" and not group_by:
        sql = f"SELECT COUNT(*) AS CNT FROM {table_literal}"
        if where_clause:
            sql += f"\n{where_clause}"
        return sql, binds

    if group_by:
        group_expr = group_by
        if isinstance(group_expr, str):
            group_expr = group_expr.strip()
        if not group_expr:
            return None, {}

        if isinstance(group_expr, str) and group_expr.upper() == "OWNER_DEPARTMENT":
            group_expr = "NVL(OWNER_DEPARTMENT, '(Unknown)')"

        if agg == "count":
            measure_alias = "CNT"
            select_measure = "COUNT(*) AS CNT"
        else:
            expr = measure_sql or _VALUE_NET
            agg_func = agg.upper() if agg else "SUM"
            measure_alias = "MEASURE"
            select_measure = f"{agg_func}({expr}) AS {measure_alias}"

        lines = [
            "SELECT",
            f"  {group_expr} AS GROUP_KEY,",
            f"  {select_measure}",
            f"FROM {table_literal}",
        ]
        if where_clause:
            lines.append(where_clause)
        lines.append(f"GROUP BY {group_expr}")

        order_expr = None
        if sort_by:
            order_expr = sort_by
            if order_expr.upper() == "MEASURE":
                order_expr = measure_alias
        elif agg == "count":
            order_expr = "CNT"
        elif user_requested_top_n:
            order_expr = measure_alias

        if order_expr:
            direction = "DESC" if (sort_desc is True or (sort_desc is None and (agg != "count" or user_requested_top_n))) else "ASC"
            lines.append(f"ORDER BY {order_expr} {direction}")

        if user_requested_top_n and isinstance(top_n, int) and top_n > 0:
            binds["top_n"] = int(top_n)
            lines.append("FETCH FIRST :top_n ROWS ONLY")

        return "\n".join(lines), binds

    if agg in {"sum", "avg", "min", "max"} or (measure_sql and agg in {None, ""}):
        expr = measure_sql or _VALUE_NET
        agg_func = agg.upper() if agg else "SUM"
        alias = "MEASURE"
        sql_lines = [
            f"SELECT {agg_func}({expr}) AS {alias}",
            f"FROM {table_literal}",
        ]
        if where_clause:
            sql_lines.append(where_clause)
        return "\n".join(sql_lines), binds

    if wants_all or wants_all is None:
        order_column = sort_by or date_col
        direction = "DESC" if (sort_desc or (user_requested_top_n and sort_desc is None)) else "ASC"
        lines = [f"SELECT * FROM {table_literal}"]
        if where_clause:
            lines.append(where_clause)
        lines.append(f"ORDER BY {order_column} {direction}")
        if user_requested_top_n and isinstance(top_n, int) and top_n > 0:
            binds["top_n"] = int(top_n)
            lines.append("FETCH FIRST :top_n ROWS ONLY")
        return "\n".join(lines), binds

    return None, {}
