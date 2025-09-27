from __future__ import annotations
import re
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
from word2number.w2n import word_to_num
from .types import NLIntent
from .sql_fragments import expr_net, expr_gross


_TOP = re.compile(r'\btop\s+(\d+|\w+)\b', re.I)
_LAST_N = re.compile(r'\blast\s+(\d+|\w+)\s+(month|months|day|days|week|weeks)\b', re.I)
_LAST_MONTH = re.compile(r'\blast\s+month\b', re.I)
_LAST_3_MONTHS = re.compile(r'\blast\s+3\s+months\b', re.I)
_NEXT_N_DAYS = re.compile(r'\bnext\s+(\d+|\w+)\s+days?\b', re.I)
_EXPIRE_30 = re.compile(r'\bexpir\w*\b.*\b30\b.*\bdays?\b', re.I)
_COUNT = re.compile(r'\bcount\b|\(count\)', re.I)
_GROSS = re.compile(r'\bgross\b', re.I)
_NET = re.compile(r'\bnet\b', re.I)
_BY_DEPT = re.compile(r'\b(owner\s*department|department_oul|department|oul)\b', re.I)
_BY_STATUS = re.compile(r'\bstatus\b', re.I)
_BY_ENTITY = re.compile(r'\bentity(_no)?\b', re.I)
_REQUESTED = re.compile(r'\brequested?\b|\brequest\s+date\b', re.I)
_RENEWAL_20XX = re.compile(r'\brequest\s*type\s*=\s*renewal\b.*\b20\d\d\b', re.I)
_YTD_20XX = re.compile(r'\b(?:YTD)\s*(20\d\d)\b', re.I)
_YEAR_20XX = re.compile(r'\b(20\d\d)\b', re.I)
_AVG = re.compile(r'\baverage|avg\b', re.I)
_PER = re.compile(r'\bper\b|\bby\b', re.I)
_STAKEHOLDER = re.compile(r'\bstakeholder', re.I)


def _to_int(tok: str) -> int:
    try:
        return int(tok)
    except ValueError:
        return word_to_num(tok)


def _last_month_range(today: date) -> tuple[date, date]:
    first_this = today.replace(day=1)
    last_prev = first_this - timedelta(days=1)
    first_prev = last_prev.replace(day=1)
    return first_prev, last_prev


def _last_n_range(today: date, n: int, unit: str) -> tuple[date, date]:
    end = today
    if unit.startswith('day'):
        start = end - timedelta(days=n)
    elif unit.startswith('week'):
        start = end - timedelta(weeks=n)
    else:
        start = end - relativedelta(months=n)
    return start, end


def parse_contract_intent(q: str, today: date | None = None) -> NLIntent:
    t = (q or "").strip()
    today = today or date.today()
    it = NLIntent(raw=t, wants_all_columns=True)
    it.measure_sql = expr_net()
    it.sort_by = it.measure_sql
    it.sort_desc = True

    # Top N
    m = _TOP.search(t)
    if m:
        n_tok = m.group(1)
        it.top_n = _to_int(n_tok)
        it.user_requested_top_n = True

    # Gross / Net
    if _GROSS.search(t):
        it.measure_sql = expr_gross()
        it.sort_by = it.measure_sql
    elif _NET.search(t):
        it.measure_sql = expr_net()
        it.sort_by = it.measure_sql

    # Group-by (by/per …)
    if _PER.search(t):
        if _BY_DEPT.search(t):
            # prefer OWNER_DEPARTMENT unless explicitly DEPARTMENT_OUL
            if "DEPARTMENT_OUL" in t.upper() or "OUL" in t.upper():
                it.group_by = "DEPARTMENT_OUL"
            else:
                it.group_by = "OWNER_DEPARTMENT"
        elif _BY_STATUS.search(t):
            it.group_by = "CONTRACT_STATUS"
        elif _BY_ENTITY.search(t):
            it.group_by = "ENTITY"
        elif _STAKEHOLDER.search(t):
            it.group_by = "STAKEHOLDER_UNION"  # special: 1..8 slots

    # Requested … → REQUEST_DATE
    if _REQUESTED.search(t):
        it.date_column = "REQUEST_DATE"

    # Expiring … 30 days (count)
    if _EXPIRE_30.search(t) or (_COUNT.search(t) and "expir" in t.lower()):
        it.expire = True
        it.date_column = "END_DATE"
        start = today
        end = today + timedelta(days=30)
        it.has_time_window = True
        it.explicit_dates = {"start": start, "end": end}
        it.agg = "count"
        return it

    # next N days
    m = _NEXT_N_DAYS.search(t)
    if m:
        n = _to_int(m.group(1))
        it.has_time_window = True
        it.date_column = it.date_column or "END_DATE"
        it.explicit_dates = {"start": today, "end": today + timedelta(days=n)}
        return it

    # last month
    if _LAST_MONTH.search(t):
        start, end = _last_month_range(today)
        it.has_time_window = True
        it.explicit_dates = {"start": start, "end": end}
        # If not explicitly requested, prefer overlap window
        it.date_column = it.date_column or "OVERLAP"

    # last N (days|weeks|months)
    m = _LAST_N.search(t)
    if m:
        n = _to_int(m.group(1))
        unit = m.group(2).lower()
        start, end = _last_n_range(today, n, unit)
        it.has_time_window = True
        it.explicit_dates = {"start": start, "end": end}
        it.date_column = it.date_column or ("OVERLAP" if unit.startswith("month") else "REQUEST_DATE")

    # “last 3 months” (explicit)
    if _LAST_3_MONTHS.search(t):
        start, end = _last_n_range(today, 3, "months")
        it.has_time_window = True
        it.explicit_dates = {"start": start, "end": end}
        it.date_column = it.date_column or "OVERLAP"

    # YTD
    m = _YTD_20XX.search(t)
    if m:
        year = int(m.group(1))
        start = date(year, 1, 1)
        end = today
        it.has_time_window = True
        it.explicit_dates = {"start": start, "end": end}
        it.date_column = it.date_column or "OVERLAP"
        if _GROSS.search(t):
            it.measure_sql = expr_gross()
            it.sort_by = it.measure_sql
        it.top_n = it.top_n or 5
        it.user_requested_top_n = True

    # “in 2023/2024 …”
    m = _YEAR_20XX.search(t)
    if m and _RENEWAL_20XX.search(t):
        year = int(m.group(1))
        it.has_time_window = True
        it.explicit_dates = {"start": date(year,1,1), "end": date(year,12,31)}
        it.date_column = "REQUEST_DATE"

    # Count detection
    if _COUNT.search(t) and not it.agg:
        # Only count(*) when it's truly a count question without “by”
        it.agg = "count"

    # Average …
    if _AVG.search(t) and "REQUEST_TYPE" in t.upper():
        it.agg = "avg"
        it.group_by = "REQUEST_TYPE"
        it.measure_sql = expr_gross()
        it.sort_by = None
        it.sort_desc = None

    return it
