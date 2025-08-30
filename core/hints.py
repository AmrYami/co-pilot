# core/hints.py
from __future__ import annotations
import re
from datetime import date, timedelta
from typing import Any, Dict, Optional, Tuple

LAST_MONTH = re.compile(r"\blast\s+month\b", re.I)
YTD = re.compile(r"\bYTD\b|\byear\s*to\s*date\b", re.I)
RANGE_YM   = re.compile(r"\bfrom\s+(\d{4})[-/](\d{1,2})\s+to\s+(\d{4})[-/](\d{1,2})\b", re.I)
RANGE_YMD  = re.compile(r"\bfrom\s+(\d{4})[-/](\d{1,2})[-/](\d{1,2})\s+to\s+(\d{4})[-/](\d{1,2})[-/](\d{1,2})\b", re.I)
BETWEEN_YMD= re.compile(r"\bbetween\s+(\d{4})-(\d{2})-(\d{2})\s+and\s+(\d{4})-(\d{2})-(\d{2})\b", re.I)

def _month_bounds(y: int, m: int) -> Tuple[date, date]:
    from calendar import monthrange
    s = date(y, m, 1)
    e = date(y, m, monthrange(y, m)[1])
    return s, e

def infer_date_range(text: str, today: Optional[date] = None) -> Optional[Tuple[date, date, str]]:
    """Return (start, end, grain) if we can infer a range. Grain ∈ {'day','month','year'}."""
    t = text.strip()
    d = today or date.today()

    m = RANGE_YMD.search(t)
    if m:
        y1, m1, d1, y2, m2, d2 = map(int, m.groups())
        return date(y1, m1, d1), date(y2, m2, d2), "day"

    m = BETWEEN_YMD.search(t)
    if m:
        y1, m1, d1, y2, m2, d2 = map(int, m.groups())
        return date(y1, m1, d1), date(y2, m2, d2), "day"

    m = RANGE_YM.search(t)
    if m:
        y1, m1, y2, m2 = map(int, m.groups())
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

EQ_FILTER = re.compile(r"\b([A-Za-z_][A-Za-z0-9_\.]*)\s*[:=]\s*([A-Za-z0-9_\-./]+)\b")

def extract_eq_filters(text: str) -> Dict[str, str]:
    """Very light heuristic for col:value mentions. App adapter can post-process."""
    out: Dict[str,str] = {}
    for m in EQ_FILTER.finditer(text):
        out[m.group(1)] = m.group(2)
    return out

def make_hints(question: str) -> Dict[str, Any]:
    hints: Dict[str,Any] = {}
    dr = infer_date_range(question)
    if dr:
        s,e,g = dr
        hints["date_range"] = {"start": str(s), "end": str(e), "grain": g}
    eqs = extract_eq_filters(question)
    if eqs:
        hints["eq_filters"] = eqs
    return hints
