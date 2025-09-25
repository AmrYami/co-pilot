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


_TOP = re.compile(r"\btop\s*(\d+)\b", re.I)
_COUNT = re.compile(r"\bcount\b|\(count\)", re.I)
_GROSS = re.compile(r"\bgross\b", re.I)
_NET = re.compile(r"\bnet\b|\bcontract value\b", re.I)
_BY = re.compile(r"\bby\s+([a-zA-Z_ ]+)|\bper\s+([a-zA-Z_ ]+)", re.I)
_EXPIRE = re.compile(r"\bexpir\w*\b", re.I)
_LAST_N_MONTHS = re.compile(r"\blast\s+(\d+)\s+months?\b", re.I)
_LAST_MONTH = re.compile(r"\blast\s+month\b", re.I)
_LAST_N_DAYS = re.compile(r"\blast\s+(\d+)\s+days?\b", re.I)
_THREE_MONTHS = re.compile(r"\blast\s+3\s+months?\b", re.I)
_BY_STATUS = re.compile(r"\bby\s+status\b", re.I)
_LIST_COLS = re.compile(r"\(([^)]+)\)\s*$")

# Date / dimension cues
_REQ_CUES = re.compile(r"\b(request(ed)?|requested|request date|submitted)\b", re.I)
_START_CUES = re.compile(r"\b(start(s|ed|ing)?|begin(s|ning)?)\b", re.I)
_END_CUES = re.compile(r"\b(end(s|ed|ing)?|expire(s|d|ing)?|expiry|expiring)\b", re.I)
_CONTRACTS = re.compile(r"\bcontract(s)?\b", re.I)
_STAKE = re.compile(r"\bstakeholder(s)?\b", re.I)
_DEPT = re.compile(r"\b(owner\s*department|department)\b", re.I)
_OUL = re.compile(r"\b(oul|manager|department_oul)\b", re.I)
_ENTITY = re.compile(r"\b(entity|entity\s*no)\b", re.I)


@dataclass
class NLIntent:
    date_column: Optional[str] = None
    explicit_dates: Optional[Dict[str, str]] = None
    has_time_window: Optional[bool] = None
    top_n: Optional[int] = None
    sort_by: Optional[str] = None
    sort_desc: Optional[bool] = None
    group_by: Optional[str] = None
    agg: Optional[str] = None
    measure_sql: Optional[str] = None
    wants_all_columns: Optional[bool] = None
    user_requested_top_n: Optional[bool] = None
    notes: Dict[str, Any] = field(default_factory=dict)
    expire: Optional[bool] = None


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
    if "oul" in d or "manager" in d:
        return "DEPARTMENT_OUL"
    return None


def _set_window(intent: NLIntent, start: str, end: str) -> None:
    intent.has_time_window = True
    intent.explicit_dates = {"start": start, "end": end}


def parse_intent(q: str) -> NLIntent:
    q = (q or "").strip()
    now = today_utc()
    intent = NLIntent(notes={"q": q})

    m = _TOP.search(q)
    if m:
        intent.top_n = int(m.group(1))
        intent.user_requested_top_n = True

    if _COUNT.search(q):
        intent.agg = "count"

    if _GROSS.search(q):
        intent.measure_sql = (
            "NVL(CONTRACT_VALUE_NET_OF_VAT,0) + CASE WHEN NVL(VAT,0) BETWEEN 0 AND 1 "
            "THEN NVL(CONTRACT_VALUE_NET_OF_VAT,0) * NVL(VAT,0) ELSE NVL(VAT,0) END"
        )
    else:
        intent.measure_sql = "NVL(CONTRACT_VALUE_NET_OF_VAT,0)"

    if _STAKE.search(q):
        intent.group_by = "CONTRACT_STAKEHOLDER_1"
    elif _DEPT.search(q):
        intent.group_by = "OWNER_DEPARTMENT"
    elif _OUL.search(q):
        intent.group_by = "DEPARTMENT_OUL"
    elif _ENTITY.search(q):
        intent.group_by = "ENTITY_NO"
    else:
        m = _BY.search(q)
        if m:
            dim = (m.group(1) or m.group(2) or "").strip()
            gb = normalize_dimension(dim)
            if gb:
                intent.group_by = gb
        elif _BY_STATUS.search(q):
            intent.group_by = "CONTRACT_STATUS"
            if intent.agg is None:
                intent.agg = "count"

    intent.sort_by = intent.measure_sql
    intent.sort_desc = True

    if _EXPIRE.search(q):
        intent.expire = True

    if _LAST_MONTH.search(q):
        start, end = last_month(now)
        _set_window(intent, start, end)
    elif _THREE_MONTHS.search(q):
        start, end = last_n_months(3, now)
        _set_window(intent, start, end)
    else:
        m = _LAST_N_MONTHS.search(q)
        if m:
            n = int(m.group(1))
            start, end = last_n_months(n, now)
            _set_window(intent, start, end)
        m = _LAST_N_DAYS.search(q)
        if m:
            n = int(m.group(1))
            start, end = last_n_days(n, now)
            _set_window(intent, start, end)

    if intent.expire and not intent.explicit_dates:
        start, end = last_n_days(30, now)
        _set_window(intent, start, end)

    if mentions_requested(q) or _REQ_CUES.search(q):
        intent.date_column = "REQUEST_DATE"
    elif _START_CUES.search(q):
        intent.date_column = "START_DATE"
    elif intent.expire or _END_CUES.search(q):
        intent.date_column = "END_DATE"
    else:
        intent.date_column = "OVERLAP"

    if intent.expire:
        intent.has_time_window = True

    if intent.wants_all_columns is None:
        intent.wants_all_columns = intent.group_by is None

    m = _LIST_COLS.search(q)
    if m:
        cols = [c.strip().upper().replace(" ", "_") for c in m.group(1).split(",") if c.strip()]
        if cols:
            intent.notes["projection"] = cols
            intent.wants_all_columns = False

    return intent
