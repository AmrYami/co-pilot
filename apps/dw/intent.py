from __future__ import annotations
import re
from typing import Optional, List, Dict, Any
from pydantic import BaseModel
from word2number import w2n
from dateutil.relativedelta import relativedelta
from datetime import date, timedelta

# NOTE: English-first parsing. Arabic can be added later once EN is rock-solid.


class NLIntent(BaseModel):
    # Core semantic slots we need downstream
    has_time_window: Optional[bool] = None
    explicit_dates: Optional[Dict[str, str]] = None  # {"start": "YYYY-MM-DD", "end": "YYYY-MM-DD"}
    date_column: Optional[str] = None                # "REQUEST_DATE" | "OVERLAP" | "END_DATE" for expiry
    expire: Optional[int] = None                     # days-ahead when "(expiring in N days)"
    group_by: Optional[str] = None                   # e.g., "OWNER_DEPARTMENT", "CONTRACT_STATUS"
    agg: Optional[str] = None                        # "count" | "sum" | "avg"
    measure_sql: Optional[str] = None                # Oracle expr e.g. NVL(CONTRACT_VALUE_NET_OF_VAT,0)
    sort_by: Optional[str] = None                    # column/expression to sort
    sort_desc: Optional[bool] = None
    top_n: Optional[int] = None
    user_requested_top_n: Optional[bool] = None
    wants_all_columns: Optional[bool] = None
    # Full-text search hook
    full_text_search: Optional[bool] = None
    fts_tokens: Optional[List[str]] = None
    notes: Dict[str, Any] = {}


_RE_REQUESTED = re.compile(r'\b(requested|request\s+date|request_date|request type|request\s*type)\b', re.I)
_RE_COUNT     = re.compile(r'\b(count|how many|number of)\b', re.I)
_RE_BY        = re.compile(r'\bby\s+([a-zA-Z0-9_ ]+)\b', re.I)
_RE_PER       = re.compile(r'\bper\s+([a-zA-Z0-9_ ]+)\b', re.I)
_RE_TOPN      = re.compile(r'\btop\s+([0-9]+|one|two|three|four|five|six|seven|eight|nine|ten|twenty|thirty|forty|fifty)\b', re.I)
_RE_LAST_N_MONTHS = re.compile(r'\blast\s+([0-9]+|one|two|three|four|five|six|seven|eight|nine|ten|twelve)\s+months?\b', re.I)
_RE_LAST_MONTH    = re.compile(r'\blast\s+month\b', re.I)
_RE_LAST_6_MONTHS = re.compile(r'\blast\s+6\s+months?\b', re.I)
_RE_90_DAYS       = re.compile(r'\b(last|next)\s+90\s+days?\b', re.I)
_RE_NEXT_N_DAYS   = re.compile(r'\bexpir(?:y|ing)\s+in\s+([0-9]+)\s+days?\b', re.I)
_RE_YEAR_YYYY     = re.compile(r'\b(20\d{2})\b')
_RE_RENEWAL       = re.compile(r'\brenewal\b', re.I)
_RE_GROSS         = re.compile(r'\bgross\b', re.I)
_RE_NET           = re.compile(r'\bnet\b', re.I)


def _num_from_word_or_digit(s: str) -> Optional[int]:
    try:
        return int(s)
    except Exception:
        pass
    try:
        return w2n.word_to_num(s.lower())
    except Exception:
        return None


def _month_bounds(d: date) -> tuple[date, date]:
    start = d.replace(day=1)
    next_month = start + relativedelta(months=1)
    end = next_month - timedelta(days=1)
    return start, end


def _last_month_bounds(today: date) -> tuple[date, date]:
    first_this, _ = _month_bounds(today)
    last_month_end = first_this - timedelta(days=1)
    last_month_start = last_month_end.replace(day=1)
    return last_month_start, last_month_end


def _last_n_months_bounds(today: date, n: int) -> tuple[date, date]:
    end = today
    start = (today.replace(day=1) - relativedelta(months=n))  # inclusive from month start n months ago
    return start, end


def _ytd_bounds(year: int, today: date) -> tuple[date, date]:
    start = date(year, 1, 1)
    end = today if today.year == year else date(year, 12, 31)
    return start, end


def parse_intent(question: str, *, today: Optional[date] = None, wants_all_columns_default: bool = True) -> NLIntent:
    q = (question or "").strip()
    today = today or date.today()
    it = NLIntent(notes={"q": q}, wants_all_columns=wants_all_columns_default)

    # 1) windows
    if _RE_LAST_MONTH.search(q):
        it.has_time_window = True
        s, e = _last_month_bounds(today)
        it.explicit_dates = {"start": s.isoformat(), "end": e.isoformat()}
    m = _RE_LAST_N_MONTHS.search(q)
    if m:
        it.has_time_window = True
        n = _num_from_word_or_digit(m.group(1)) or 3
        s, e = _last_n_months_bounds(today, n)
        it.explicit_dates = {"start": s.isoformat(), "end": e.isoformat()}
    if _RE_LAST_6_MONTHS.search(q):
        it.has_time_window = True
        s, e = _last_n_months_bounds(today, 6)
        it.explicit_dates = {"start": s.isoformat(), "end": e.isoformat()}
    m = _RE_NEXT_N_DAYS.search(q)
    if m:
        days = _num_from_word_or_digit(m.group(1)) or 30
        it.has_time_window = True
        it.expire = days
        it.explicit_dates = {"start": today.isoformat(), "end": (today + timedelta(days=days)).isoformat()}
        it.date_column = "END_DATE"  # expiry asks about END_DATE
    if _RE_90_DAYS.search(q) and not it.explicit_dates:
        # last/next 90 days (default next for "expiring") — if "expiring" present we set above
        it.has_time_window = True
        s = today - timedelta(days=90)
        it.explicit_dates = {"start": s.isoformat(), "end": today.isoformat()}

    # 2) year filters
    year_match = _RE_YEAR_YYYY.search(q)
    if year_match and "ytd" not in q.lower():
        yy = int(year_match.group(1))
        it.has_time_window = True
        it.explicit_dates = {"start": date(yy, 1, 1).isoformat(), "end": date(yy, 12, 31).isoformat()}
    if "ytd" in q.lower():
        yy = today.year
        s, e = _ytd_bounds(yy, today)
        it.has_time_window = True
        it.explicit_dates = {"start": s.isoformat(), "end": e.isoformat()}

    # 3) date column decision:
    # Default to OVERLAP unless user explicitly talks about "requested".
    if it.date_column is None:
        it.date_column = "REQUEST_DATE" if _RE_REQUESTED.search(q) else "OVERLAP"

    # 4) top N
    m = _RE_TOPN.search(q)
    if m:
        n = _num_from_word_or_digit(m.group(1)) or 10
        it.top_n = n
        it.user_requested_top_n = True

    # 5) metrics
    # default measure: net value; "gross" uses net + VAT (handling rate vs absolute)
    if _RE_GROSS.search(q):
        it.measure_sql = (
            "NVL(CONTRACT_VALUE_NET_OF_VAT,0) + "
            "CASE WHEN NVL(VAT,0) BETWEEN 0 AND 1 "
            "THEN NVL(CONTRACT_VALUE_NET_OF_VAT,0)*NVL(VAT,0) ELSE NVL(VAT,0) END"
        )
    else:
        it.measure_sql = "NVL(CONTRACT_VALUE_NET_OF_VAT,0)"

    # 6) aggregations
    if _RE_COUNT.search(q) or "(count)" in q.lower():
        it.agg = "count"

    # 7) group-by phrases ("by X" / "per X")
    m = _RE_BY.search(q) or _RE_PER.search(q)
    if m:
        dim = m.group(1).strip().lower()
        # quick mapping
        if "department" in dim and "owner" in dim:
            it.group_by = "OWNER_DEPARTMENT"
        elif dim == "department":
            it.group_by = "OWNER_DEPARTMENT"
        elif "entity" in dim and "no" in dim:
            it.group_by = "ENTITY_NO"
        elif "entity" in dim:
            it.group_by = "ENTITY"
        elif "owner" in dim:
            it.group_by = "CONTRACT_OWNER"
        elif "stakeholder" in dim:
            it.group_by = "CONTRACT_STAKEHOLDER_1"  # builder will upcast to 1..N slots if requested
        elif "status" in dim:
            it.group_by = "CONTRACT_STATUS"

    # 8) sort desc by measure when "top" present
    if it.top_n:
        it.sort_by = it.measure_sql
        it.sort_desc = True

    return it
