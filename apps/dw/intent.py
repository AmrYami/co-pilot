from __future__ import annotations
from dataclasses import dataclass
from calendar import monthrange
from datetime import date, timedelta
try:  # pragma: no cover - optional dependency
    from dateutil.relativedelta import relativedelta
except Exception:  # pragma: no cover - optional dependency
    relativedelta = None  # type: ignore[assignment]
import re
from typing import Optional, Literal

# --------- Public shape the rest of the app uses ----------
WindowKind = Literal["start_only", "end_only", "overlap"]


@dataclass
class DWIntent:
    q: str
    # core
    agg: Optional[str] = None            # 'count' | 'sum' | 'avg' | None
    measure_sql: Optional[str] = None    # e.g., NVL(CONTRACT_VALUE_NET_OF_VAT,0) or gross formula
    group_by: Optional[str] = None       # column to group by if "by/per …"
    sort_by: Optional[str] = None
    sort_desc: bool = False
    top_n: Optional[int] = None
    user_requested_top_n: bool = False
    wants_all_columns: bool = True

    # time
    has_time_window: bool = False
    explicit_dates: Optional[dict] = None  # {"start": "YYYY-MM-DD", "end": "YYYY-MM-DD"}
    horizon_days: Optional[int] = None
    # date semantics
    date_column: Optional[str] = None    # REQUEST_DATE | START_DATE | END_DATE (only for start_only/end_only)
    window_kind: WindowKind = "overlap"  # 'overlap' for active contracts in window


# --------- Keyword lexicons ----------
RE_EXPIRE = re.compile(r'\b(expir(?:e|ing|y)|ending|due)\b', re.I)
RE_START  = re.compile(r'\b(start(?:s|ed|ing)?|signed|activate(?:d)?)\b', re.I)
RE_REQUEST= re.compile(r'\b(request(?:ed)?|submitted|appl(?:y|ied))\b', re.I)
RE_BY     = re.compile(r'\bby\s+([a-z_ ]+)|per\s+([a-z_ ]+)', re.I)
RE_TOPN   = re.compile(r'\btop\s+(\d+)\b', re.I)
RE_COUNT  = re.compile(r'\bcount\b|\(count\)', re.I)
RE_GROSS  = re.compile(r'\bgross\b', re.I)
RE_NET    = re.compile(r'\bnet\b|\bcontract value\b', re.I)

# windows like "last 3 months", "next 90 days"
RE_LAST_NEXT = re.compile(r'\b(last|next)\s+(\d+)\s+(day|days|week|weeks|month|months|quarter|quarters|year|years)\b', re.I)
RE_LAST_MONTH   = re.compile(r'\blast\s+month\b', re.I)
RE_LAST_QUARTER = re.compile(r'\blast\s+quarter\b', re.I)

# synonyms to canonical DW columns
DIM_SYNONYMS = [
    (re.compile(r'\bstakeholder\b', re.I), "CONTRACT_STAKEHOLDER_1"),
    (re.compile(r'\bowner department\b|\bdepartment\b', re.I), "OWNER_DEPARTMENT"),
    (re.compile(r'\bowner\b', re.I), "CONTRACT_OWNER"),
    (re.compile(r'\bentity\b', re.I), "ENTITY_NO"),
]


def _month_bounds(d: date) -> tuple[date, date]:
    first = d.replace(day=1)
    prev_last = first - timedelta(days=1)
    prev_first = prev_last.replace(day=1)
    return prev_first, prev_last


def _quarter_bounds(d: date) -> tuple[date, date]:
    q = (d.month - 1) // 3 + 1
    # start of this quarter
    q_start = date(d.year, (3 * (q - 1)) + 1, 1)
    # last day of previous quarter
    prev_q_end = q_start - timedelta(days=1)
    # previous quarter start
    prev_q = (prev_q_end.month - 1) // 3 + 1
    prev_q_start = date(prev_q_end.year, (3 * (prev_q - 1)) + 1, 1)
    return prev_q_start, prev_q_end


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
        # handle Feb 29 -> Feb 28
        return d.replace(month=2, day=28, year=d.year + years)


def _roll_bounds(d: date, last_or_next: str, n: int, unit: str) -> tuple[date, date]:
    last = last_or_next.lower() == "last"
    unit = unit.lower()
    if unit.startswith("day"):
        if last:
            return d - timedelta(days=n), d
        else:
            return d, d + timedelta(days=n)
    if unit.startswith("week"):
        if last:
            return d - timedelta(weeks=n), d
        else:
            return d, d + timedelta(weeks=n)
    if unit.startswith("month"):
        if last:
            start_base = d.replace(day=1)
            if relativedelta:
                start = start_base - relativedelta(months=n)
            else:
                start = _shift_months_basic(start_base, -n)
            end   = d
            return start, end
        else:
            if relativedelta:
                return d, d + relativedelta(months=n)
            return d, _shift_months_basic(d, n)
    if unit.startswith("quarter"):
        months = 3 * n
        if last:
            start_base = d.replace(day=1)
            if relativedelta:
                start = start_base - relativedelta(months=months)
            else:
                start = _shift_months_basic(start_base, -months)
            end   = d
            return start, end
        else:
            if relativedelta:
                return d, d + relativedelta(months=months)
            return d, _shift_months_basic(d, months)
    if unit.startswith("year"):
        if last:
            if relativedelta:
                return d - relativedelta(years=n), d
            return _shift_years_basic(d, -n), d
        else:
            if relativedelta:
                return d, d + relativedelta(years=n)
            return d, _shift_years_basic(d, n)
    return d, d


def parse_intent(q: str, default_date_col: str = "START_DATE") -> DWIntent:
    """Heuristic DW intent:
    - choose window kind (start_only, end_only, overlap)
    - extract last/next windows (days/weeks/months/quarters/years) and calendar buckets
    - map group-by dimension
    """
    today = date.today()
    t = (q or "").strip()
    intent = DWIntent(q=t)

    # group-by
    m = RE_BY.search(t)
    if m:
        grp = m.group(1) or m.group(2)
        if grp:
            for rx, col in DIM_SYNONYMS:
                if rx.search(grp):
                    intent.group_by = col
                    break

    # agg
    if RE_COUNT.search(t):
        intent.agg = "count"

    # measure: gross vs net (default to net if no keyword)
    if RE_GROSS.search(t):
        intent.measure_sql = "NVL(CONTRACT_VALUE_NET_OF_VAT,0) + CASE WHEN NVL(VAT,0) BETWEEN 0 AND 1 THEN NVL(CONTRACT_VALUE_NET_OF_VAT,0)*NVL(VAT,0) ELSE NVL(VAT,0) END"
    else:
        intent.measure_sql = "NVL(CONTRACT_VALUE_NET_OF_VAT,0)"

    # Top N
    mt = RE_TOPN.search(t)
    if mt:
        intent.top_n = int(mt.group(1))
        intent.user_requested_top_n = True
        # default sort by measure desc for topN if no explicit sort
        intent.sort_by = intent.measure_sql
        intent.sort_desc = True

    # window semantics
    if RE_EXPIRE.search(t):
        intent.window_kind = "end_only"
        intent.date_column = "END_DATE"
    elif RE_START.search(t):
        intent.window_kind = "start_only"
        intent.date_column = "START_DATE"
    elif RE_REQUEST.search(t):
        intent.window_kind = "start_only"  # single column filter
        intent.date_column = "REQUEST_DATE"
    else:
        # generic contracts + time → treat as active overlap
        intent.window_kind = "overlap"
        intent.date_column = None

    # calendar buckets
    if RE_LAST_MONTH.search(t):
        s, e = _month_bounds(today)
        intent.has_time_window = True
        intent.explicit_dates = {"start": s.isoformat(), "end": e.isoformat()}
    elif RE_LAST_QUARTER.search(t):
        s, e = _quarter_bounds(today)
        intent.has_time_window = True
        intent.explicit_dates = {"start": s.isoformat(), "end": e.isoformat()}
    else:
        mrel = RE_LAST_NEXT.search(t)
        if mrel:
            which, n, unit = mrel.groups()
            n = int(n)
            s, e = _roll_bounds(today, which, n, unit)
            intent.has_time_window = True
            intent.explicit_dates = {"start": s.isoformat(), "end": e.isoformat()}

    # If we still don't have any time window but the user said "last/next X" with no match above,
    # you could drop to the clarifier. We keep intent as-is and let caller decide.

    # default date column if we landed on start_only without explicit, or if caller requests it
    if intent.window_kind in ("start_only", "end_only") and not intent.date_column:
        intent.date_column = default_date_col
    elif intent.window_kind == "overlap" and not intent.has_time_window and default_date_col:
        intent.date_column = default_date_col

    # Group-by heuristics: default to aggregates to avoid projecting every column.
    if intent.group_by and not intent.agg:
        if RE_COUNT.search(t):
            intent.agg = "count"
        elif intent.group_by == "CONTRACT_STATUS":
            intent.agg = "count"
        else:
            intent.agg = "sum"

    # wants_all_columns only when not aggregated
    if intent.group_by or intent.agg in ("count", "sum", "avg"):
        intent.wants_all_columns = False
    else:
        intent.wants_all_columns = True

    return intent
