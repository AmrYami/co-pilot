from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Dict, Optional

from .utils import (
    last_month,
    last_n_days,
    last_n_months,
    mentions_requested,
    today_utc,
)


_TOP_N = re.compile(r"\btop\s+(\d+)\b", re.I)
_COUNT = re.compile(r"\bcount\b|\(count\)", re.I)
_GROSS = re.compile(r"\bgross\b", re.I)
_NET = re.compile(r"\bnet\b|\bcontract value\b", re.I)
_BY = re.compile(r"\bby\s+([a-zA-Z_ ]+)|\bper\s+([a-zA-Z_ ]+)", re.I)
_EXPIRE = re.compile(r"\bexpiring?\b|\bexpire\b|\bexpires\b", re.I)
_LAST_N_MONTHS = re.compile(r"\blast\s+(\d+)\s+months?\b", re.I)
_LAST_MONTH = re.compile(r"\blast\s+month\b", re.I)
_LAST_N_DAYS = re.compile(r"\blast\s+(\d+)\s+days?\b", re.I)
_THREE_MONTHS = re.compile(r"\blast\s+3\s+months?\b", re.I)
_BY_STATUS = re.compile(r"\bby\s+status\b", re.I)
_LIST_COLS = re.compile(r"\(([^)]+)\)\s*$")


@dataclass
class NLIntent:
    # core
    has_time_window: Optional[bool] = None
    date_column: Optional[str] = None
    explicit_dates: Optional[Dict[str, str]] = None
    top_n: Optional[int] = None
    agg: Optional[str] = None
    group_by: Optional[str] = None
    sort_by: Optional[str] = None
    sort_desc: Optional[bool] = None
    wants_all_columns: Optional[bool] = None
    user_requested_top_n: Optional[bool] = None
    # semantics
    measure_sql: Optional[str] = None
    expire: Optional[bool] = None
    notes: Dict[str, Any] = field(default_factory=dict)


def normalize_dimension(dim: str) -> Optional[str]:
    d = (dim or "").strip().lower()
    if not d:
        return None
    if "stakeholder" in d:
        return "CONTRACT_STAKEHOLDER_1"
    if "owner department" in d or d == "department":
        return "OWNER_DEPARTMENT"
    if "entity" in d:
        return "ENTITY_NO"
    if "owner" in d:
        return "CONTRACT_OWNER"
    if "status" in d:
        return "CONTRACT_STATUS"
    return None


def parse_intent(q: str) -> NLIntent:
    q = (q or "").strip()
    now = today_utc()
    it = NLIntent()
    it.notes["q"] = q

    m = _TOP_N.search(q)
    if m:
        it.top_n = int(m.group(1))
        it.user_requested_top_n = True

    if _COUNT.search(q):
        it.agg = "count"

    if _GROSS.search(q):
        it.measure_sql = (
            "NVL(CONTRACT_VALUE_NET_OF_VAT,0) + "
            "CASE WHEN NVL(VAT,0) BETWEEN 0 AND 1 "
            "THEN NVL(CONTRACT_VALUE_NET_OF_VAT,0) * NVL(VAT,0) "
            "ELSE NVL(VAT,0) END"
        )
    elif _NET.search(q):
        it.measure_sql = "NVL(CONTRACT_VALUE_NET_OF_VAT,0)"

    m = _BY.search(q)
    if m:
        dim = m.group(1) or m.group(2) or ""
        it.group_by = normalize_dimension(dim)
    elif _BY_STATUS.search(q):
        it.group_by = "CONTRACT_STATUS"
        if it.agg is None:
            it.agg = "count"

    if _EXPIRE.search(q):
        it.expire = True
        it.has_time_window = True
        start, end = last_n_days(30, now)
        it.explicit_dates = {"start": start, "end": end}
        it.date_column = "END_DATE"

    if _LAST_MONTH.search(q):
        it.has_time_window = True
        start, end = last_month(now)
        it.explicit_dates = {"start": start, "end": end}
    elif _THREE_MONTHS.search(q):
        it.has_time_window = True
        start, end = last_n_months(3, now)
        it.explicit_dates = {"start": start, "end": end}
    else:
        m = _LAST_N_MONTHS.search(q)
        if m:
            it.has_time_window = True
            n = int(m.group(1))
            start, end = last_n_months(n, now)
            it.explicit_dates = {"start": start, "end": end}
        m = _LAST_N_DAYS.search(q)
        if m:
            it.has_time_window = True
            n = int(m.group(1))
            start, end = last_n_days(n, now)
            it.explicit_dates = {"start": start, "end": end}

    if mentions_requested(q):
        it.date_column = it.date_column or "REQUEST_DATE"
    else:
        if it.has_time_window and not it.expire:
            it.date_column = it.date_column or None

    if it.wants_all_columns is None:
        it.wants_all_columns = True
    if it.sort_desc is None:
        it.sort_desc = True
    if (it.measure_sql is None and it.agg in ("sum", "avg")) or ("top" in q.lower()):
        it.measure_sql = "NVL(CONTRACT_VALUE_NET_OF_VAT,0)"

    if it.user_requested_top_n and not it.group_by:
        it.group_by = "CONTRACT_STAKEHOLDER_1"

    m = _LIST_COLS.search(q)
    if m:
        cols = [c.strip().upper().replace(" ", "_") for c in m.group(1).split(",")]
        it.notes["projection"] = cols
        it.wants_all_columns = False

    return it
