# core/hints.py
from __future__ import annotations
import re
from datetime import date, timedelta
from typing import Any, Dict, Optional, Tuple

"""
Lightweight heuristics to extract hints from free text:
- Absolute date ranges (YYYY-MM-DD, DD/MM/YYYY, etc.) → day grain
- Relative ranges ('last month', 'YTD', 'last 7 days') → resolved window
- Simple eq-like filters ("for customer ABC", "status paid")

This module is app-agnostic.
"""

LAST_MONTH   = re.compile(r"\blast\s+month\b", re.I)
YTD          = re.compile(r"\bYTD\b|\byear\s*to\s*date\b", re.I)
RANGE_YM     = re.compile(r"\bfrom\s+(\d{4})[-/](\d{1,2})\s+to\s+(\d{4})[-/](\d{1,2})\b", re.I)
RANGE_YMD    = re.compile(r"\bfrom\s+(\d{4})[-/](\d{1,2})[-/](\d{1,2})\s+to\s+(\d{4})[-/](\d{1,2})[-/](\d{1,2})\b", re.I)
BETWEEN_YMD  = re.compile(r"\bbetween\s+(\d{4})-(\d{2})-(\d{2})\s+and\s+(\d{4})-(\d{2})-(\d{2})\b", re.I)

_DATE1 = r"(20\d{2})[-/\.](0?[1-9]|1[0-2])[-/\.](0?[1-9]|[12]\d|3[01])"  # 2025-08-01
_DATE2 = r"(0?[1-9]|[12]\d|3[01])[-/\.](0?[1-9]|1[0-2])[-/\.](20\d{2})"  # 01/08/2025


# very light equalities: name:ACME, status=paid, customer_id  =  42
KV_COLON     = re.compile(r"\b([A-Za-z_][A-Za-z0-9_\.]*)\s*:\s*([^\s,;]+)")
KV_EQ        = re.compile(r"\b([A-Za-z_][A-Za-z0-9_\.]*)\s*=\s*(['\"][^'\"]+['\"]|[^\s,;]+)")

def _month_bounds(y: int, m: int) -> tuple[date, date]:
    from calendar import monthrange
    s = date(y, m, 1)
    e = date(y, m, monthrange(y, m)[1])
    return s, e

def infer_date_range(text: str, today: Optional[date] = None) -> Optional[tuple[date, date, str]]:
    t = text.strip()
    d = today or date.today()

    m = RANGE_YMD.search(t)
    if m:
        y1,m1,d1,y2,m2,d2 = map(int, m.groups())
        return date(y1,m1,d1), date(y2,m2,d2), "day"

    m = BETWEEN_YMD.search(t)
    if m:
        y1,m1,d1,y2,m2,d2 = map(int, m.groups())
        return date(y1,m1,d1), date(y2,m2,d2), "day"

    m = RANGE_YM.search(t)
    if m:
        y1,m1,y2,m2 = map(int, m.groups())
        s, _ = _month_bounds(y1, m1)
        _, e = _month_bounds(y2, m2)
        return s, e, "month"

    if LAST_MONTH.search(t):
        y = d.year
        m = d.month - 1 or 12
        if d.month == 1: y -= 1
        s, e = _month_bounds(y, m)
        return s, e, "month"

    if YTD.search(t):
        s = date(d.year, 1, 1)
        return s, d, "day"

    return None

def parse_eq_filters(text: str) -> dict[str, str]:
    eq: dict[str, str] = {}
    for m in KV_COLON.finditer(text):
        k, v = m.group(1).strip(), m.group(2).strip().strip('"\'')
        eq[k] = v
    for m in KV_EQ.finditer(text):
        k, v = m.group(1).strip(), m.group(2).strip().strip('"\'')
        eq[k] = v
    return eq

def make_hints(text: str, *, today: Optional[date] = None) -> Dict[str, Any]:

    """Extract light-weight intent from free text (date range + eq filters)."""
    out: Dict[str, Any] = {}
    dr = infer_date_range(text, today=today)
    if dr:
        s, e, grain = dr
        out["date_range"] = {"start": s.isoformat(), "end": e.isoformat(), "grain": grain}
    eq = parse_eq_filters(text)
    if eq:
        out["eq_filters"] = eq
    return out


def _mk_date(y: str, m: str, d: str) -> date:
    return date(int(y), int(m), int(d))

def _parse_absolute_range(s: str) -> Optional[Tuple[date, date]]:
    # YYYY-MM-DD .. YYYY-MM-DD
    m = re.search(_DATE1 + r"\s*(to|-|…|—|–)\s*" + _DATE1, s)
    if m:
        y1, m1, d1, y2, m2, d2 = m.group(1,2,3,4,5,6)
        return _mk_date(y1,m1,d1), _mk_date(y2,m2,d2)
    # DD/MM/YYYY .. DD/MM/YYYY
    m = re.search(_DATE2 + r"\s*(to|-|…|—|–)\s*" + _DATE2, s)
    if m:
        d1, m1, y1, d2, m2, y2 = m.group(1,2,3,4,5,6)
        return _mk_date(y1,m1,d1), _mk_date(y2,m2,d2)
    return None

def _relative_range(s: str) -> Optional[Tuple[date, date, str]]:
    t = s.lower()
    today = date.today()
    if "last month" in t:
        first_this = today.replace(day=1)
        last_prev = first_this - timedelta(days=1)
        first_prev = last_prev.replace(day=1)
        return first_prev, last_prev, "month"
    if "ytd" in t or "year to date" in t:
        start = today.replace(month=1, day=1)
        return start, today, "day"
    if "last 7 days" in t:
        return today - timedelta(days=7), today, "day"
    return None