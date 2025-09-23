from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
import re
from typing import Any, Dict, Optional, Tuple
from calendar import monthrange

try:
    import dateparser  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    dateparser = None

try:  # pragma: no cover - optional dependency
    from dateutil.relativedelta import relativedelta
except Exception:  # pragma: no cover - graceful fallback when dateutil missing
    relativedelta = None  # type: ignore[assignment]

COL_START = "START_DATE"
COL_END = "END_DATE"
COL_REQ = "REQUEST_DATE"
DEFAULT_TABLE = '"Contract"'

DIM_MAP = {
    "owner department": "OWNER_DEPARTMENT",
    "department": "OWNER_DEPARTMENT",
    "entity": "ENTITY_NO",
    "owner": "CONTRACT_OWNER",
    "stakeholder": "CONTRACT_STAKEHOLDER_1",
}

GROSS_EXPR = (
    "NVL(CONTRACT_VALUE_NET_OF_VAT,0) + CASE WHEN NVL(VAT,0) BETWEEN 0 AND 1 "
    "THEN NVL(CONTRACT_VALUE_NET_OF_VAT,0) * NVL(VAT,0) ELSE NVL(VAT,0) END"
)
NET_EXPR = "NVL(CONTRACT_VALUE_NET_OF_VAT,0)"


@dataclass
class DWIntent:
    agg: Optional[str] = None
    group_by: Optional[str] = None
    measure_sql: Optional[str] = None
    sort_by: Optional[str] = None
    sort_desc: bool = False
    top_n: Optional[int] = None
    user_requested_top_n: bool = False
    wants_all_columns: bool = True
    use_requested_date: bool = False
    expiring_days: Optional[int] = None
    window_start: Optional[date] = None
    window_end: Optional[date] = None
    notes: Dict[str, Any] = field(default_factory=dict)


def _today() -> date:
    return date.today()


def _shift_months_basic(d: date, months: int) -> date:
    month_index = d.month - 1 + months
    year = d.year + month_index // 12
    month = (month_index % 12) + 1
    day = min(d.day, monthrange(year, month)[1])
    return date(year, month, day)


def _shift_years_basic(d: date, years: int) -> date:
    try:
        return d.replace(year=d.year + years)
    except ValueError:
        # handle Feb 29 in leap years gracefully
        return d.replace(month=2, day=28, year=d.year + years)


def _parse_last_n(expr: str) -> Optional[Tuple[int, str]]:
    match = re.search(
        r"last\s+(\d+)\s+(day|days|week|weeks|month|months|quarter|quarters|year|years)",
        expr,
        re.IGNORECASE,
    )
    if not match:
        return None
    return int(match.group(1)), match.group(2).lower()


def _parse_next_n(expr: str) -> Optional[Tuple[int, str]]:
    match = re.search(
        r"next\s+(\d+)\s+(day|days|week|weeks|month|months|quarter|quarters|year|years)",
        expr,
        re.IGNORECASE,
    )
    if not match:
        return None
    return int(match.group(1)), match.group(2).lower()


def _calc_window_from_last(n: int, unit: str, today: date) -> Tuple[date, date]:
    if unit.startswith("day"):
        return today - timedelta(days=n), today
    if unit.startswith("week"):
        return today - timedelta(days=7 * n), today
    if unit.startswith("month"):
        if relativedelta:
            return today - relativedelta(months=n), today
        return _shift_months_basic(today, -n), today
    if unit.startswith("quarter"):
        if relativedelta:
            return today - relativedelta(months=3 * n), today
        return _shift_months_basic(today, -3 * n), today
    if unit.startswith("year"):
        if relativedelta:
            return today - relativedelta(years=n), today
        return _shift_years_basic(today, -n), today
    return today - timedelta(days=30), today


def _calendar_last_month(today: date) -> Tuple[date, date]:
    first_this = today.replace(day=1)
    last_prev = first_this - timedelta(days=1)
    first_prev = last_prev.replace(day=1)
    return first_prev, last_prev


def _last_quarter(today: date) -> Tuple[date, date]:
    quarter = (today.month - 1) // 3 + 1
    prev_q_end_month = (quarter - 1) * 3
    if prev_q_end_month == 0:
        prev_q_end_month = 12
        year = today.year - 1
    else:
        year = today.year
    if relativedelta:
        prev_q_end = date(year, prev_q_end_month, 1) + relativedelta(day=31)
        prev_q_start = prev_q_end - relativedelta(months=2)
        prev_q_start = prev_q_start.replace(day=1)
    else:
        prev_q_end = _shift_months_basic(date(year, prev_q_end_month, 1), 1) - timedelta(days=1)
        prev_q_start = _shift_months_basic(prev_q_end.replace(day=1), -2)
    return prev_q_start, prev_q_end


def _parse_date_text(fragment: str, *, today: date) -> Optional[date]:
    fragment = fragment.strip()
    if not fragment:
        return None
    if dateparser:
        dt = dateparser.parse(
            fragment,
            settings={
                "RELATIVE_BASE": datetime.combine(today, datetime.min.time()),
                "PREFER_DAY_OF_MONTH": "first",
            },
        )
        if dt:
            return dt.date()
    for fmt in ("%Y-%m-%d", "%d %b %Y", "%b %d %Y", "%d %B %Y", "%B %d %Y"):
        try:
            return datetime.strptime(fragment, fmt).date()
        except ValueError:
            continue
    return None


def _extract_explicit_range(text: str, *, today: date) -> Tuple[Optional[date], Optional[date]]:
    range_match = re.search(
        r"\b(?:between|from)\s+(.+?)\s+(?:and|to)\s+(.+?)(?:$|[.,;])",
        text,
        re.IGNORECASE,
    )
    if not range_match:
        return None, None
    start_raw = range_match.group(1)
    end_raw = range_match.group(2)
    start_dt = _parse_date_text(start_raw, today=today)
    end_dt = _parse_date_text(end_raw, today=today)
    return start_dt, end_dt


def parse_intent(q: str, *, today: Optional[date] = None) -> DWIntent:
    question = (q or "").strip()
    intent = DWIntent()
    intent.notes["q"] = q
    today = today or _today()

    if re.search(r"\b(request(?:ed)?|request\s+date)\b", question, re.IGNORECASE):
        intent.use_requested_date = True

    exp_match = re.search(r"\b(expiring|expires|ending|due\s+to\s+end)\b.*?(\d+)\s+days?\b", question, re.IGNORECASE)
    if exp_match:
        days_match = re.search(r"(\d+)\s+days?", question, re.IGNORECASE)
        if days_match:
            intent.expiring_days = int(days_match.group(1))

    if re.search(r"\bcount\b|\(count\)", question, re.IGNORECASE):
        intent.agg = "count"
        intent.wants_all_columns = False

    group_match = re.search(
        r"\b(?:by|per)\s+(owner department|department|entity|owner|stakeholder)s?\b",
        question,
        re.IGNORECASE,
    )
    if group_match:
        key = group_match.group(1).lower()
        intent.group_by = DIM_MAP.get(key)
        intent.wants_all_columns = False

    if re.search(r"\bgross\b", question, re.IGNORECASE):
        intent.measure_sql = GROSS_EXPR
        intent.agg = intent.agg or "sum"
        intent.sort_by = "GROSS_VALUE"
        intent.sort_desc = True
    elif re.search(r"\bnet\b|\bcontract\s+value\b", question, re.IGNORECASE):
        intent.measure_sql = NET_EXPR
        intent.agg = intent.agg or "sum"
        intent.sort_by = "NET_VALUE"
        intent.sort_desc = True

    top_match = re.search(r"\btop\s+(\d+)\b", question, re.IGNORECASE)
    if top_match:
        intent.top_n = int(top_match.group(1))
        intent.user_requested_top_n = True
        if not intent.measure_sql:
            intent.measure_sql = NET_EXPR
            intent.agg = intent.agg or "sum"
            intent.sort_by = "NET_VALUE"
            intent.sort_desc = True
        intent.wants_all_columns = False

    if re.search(r"\blast\s+month\b", question, re.IGNORECASE):
        intent.window_start, intent.window_end = _calendar_last_month(today)
    elif re.search(r"\blast\s+quarter\b", question, re.IGNORECASE):
        intent.window_start, intent.window_end = _last_quarter(today)
    else:
        last_period = _parse_last_n(question)
        if last_period:
            n, unit = last_period
            intent.window_start, intent.window_end = _calc_window_from_last(n, unit, today)
        else:
            next_period = _parse_next_n(question)
            if next_period:
                n, unit = next_period
                intent.window_start = today
                intent.window_end = _calc_window_from_last(-n, unit, today)[0]

    start_explicit, end_explicit = _extract_explicit_range(question, today=today)
    if start_explicit and end_explicit:
        intent.window_start, intent.window_end = start_explicit, end_explicit

    if intent.agg or intent.group_by:
        intent.wants_all_columns = False

    return intent


def build_sql(intent: DWIntent, *, table: str | None = None) -> Tuple[str, Dict[str, Any]]:
    binds: Dict[str, Any] = {}
    table_literal = (table or DEFAULT_TABLE) or DEFAULT_TABLE
    if not table_literal.startswith('"'):
        table_literal = f'"{table_literal.strip("\"")}"'

    where_clause = "1=1"
    if intent.expiring_days is not None:
        start_date = _today()
        end_date = start_date + timedelta(days=intent.expiring_days)
        binds["date_start"] = start_date.isoformat()
        binds["date_end"] = end_date.isoformat()
        where_clause = f"{COL_END} BETWEEN :date_start AND :date_end"
    elif intent.window_start and intent.window_end:
        binds["date_start"] = intent.window_start.isoformat()
        binds["date_end"] = intent.window_end.isoformat()
        if intent.use_requested_date:
            where_clause = f"{COL_REQ} BETWEEN :date_start AND :date_end"
        else:
            where_clause = f"{COL_END} >= :date_start AND {COL_START} <= :date_end"

    if intent.agg == "count" and not intent.group_by:
        sql = f"SELECT COUNT(*) AS CNT FROM {table_literal}"
        if where_clause != "1=1":
            sql += f"\nWHERE {where_clause}"
        return sql, binds

    if intent.group_by:
        dimension = intent.group_by
        dim_expr = f"NVL({dimension}, '(Unknown)')" if dimension == "OWNER_DEPARTMENT" else dimension
        if intent.measure_sql == GROSS_EXPR:
            measure_alias = "GROSS_VALUE"
            select_expr = f"SUM({GROSS_EXPR}) AS {measure_alias}"
        else:
            measure_alias = "NET_VALUE"
            select_expr = f"SUM({NET_EXPR}) AS {measure_alias}"
        sql_lines = [
            "SELECT",
            f"  {dim_expr} AS GROUP_KEY,",
            f"  {select_expr}",
            f"FROM {table_literal}",
        ]
        if where_clause != "1=1":
            sql_lines.append(f"WHERE {where_clause}")
        sql_lines.append(f"GROUP BY {dim_expr}")
        sql_lines.append(f"ORDER BY {measure_alias} DESC")
        if intent.top_n and intent.user_requested_top_n:
            sql_lines.append("FETCH FIRST :top_n ROWS ONLY")
            binds["top_n"] = intent.top_n
        return "\n".join(sql_lines), binds

    if intent.agg == "count":
        sql_lines = [
            "SELECT",
            "  COUNT(*) AS CNT",
            f"FROM {table_literal}",
        ]
        if where_clause != "1=1":
            sql_lines.append(f"WHERE {where_clause}")
        return "\n".join(sql_lines), binds

    select_projection = "*"
    sql_parts = [f"SELECT {select_projection}", f"FROM {table_literal}"]
    if where_clause != "1=1":
        sql_parts.append(f"WHERE {where_clause}")
        if intent.use_requested_date:
            sql_parts.append(f"ORDER BY {COL_REQ} ASC")
        else:
            sql_parts.append(f"ORDER BY {COL_START} ASC")
    if intent.top_n and intent.user_requested_top_n:
        sql_parts.append("FETCH FIRST :top_n ROWS ONLY")
        binds["top_n"] = intent.top_n
    return "\n".join(sql_parts), binds
