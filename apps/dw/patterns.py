from __future__ import annotations

from datetime import datetime, timedelta, timezone
import re
from typing import Optional, Tuple

_LAST_MONTH = re.compile(r"\blast\s+month\b", re.I)
_LAST_N_DAYS = re.compile(r"\blast\s+(\d{1,3})\s+days?\b", re.I)
_NEXT_N_DAYS = re.compile(r"\b(next|upcoming)\s+(\d{1,3})\s+days?\b", re.I)
_TOP_N = re.compile(r"\btop\s+(\d{1,3})\b", re.I)
_STAKEHOLDER = re.compile(r"\bstakeholder(s)?\b", re.I)
_DEPARTMENT = re.compile(r"\bdepartment(s)?\b", re.I)
_OWNER = re.compile(r"\bowner(s)?\b", re.I)
_EXPIRE = re.compile(r"\b(expir(y|ies|e|ing)|end(?:s|ing)?|due)\b", re.I)
_STATUS = re.compile(r"\bstatus\b", re.I)


def _month_bounds(dt: datetime) -> Tuple[datetime, datetime]:
    first_this = dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if first_this.month == 1:
        start = first_this.replace(year=first_this.year - 1, month=12)
    else:
        start = first_this.replace(month=first_this.month - 1)
    end = first_this
    return start, end


def parse_timeframe(q: str, now: Optional[datetime] = None) -> Tuple[Optional[datetime], Optional[datetime], Optional[int], Optional[int]]:
    now = now or datetime.now(timezone.utc)
    if _LAST_MONTH.search(q):
        start, end = _month_bounds(now)
        return start, end, None, None

    match = _LAST_N_DAYS.search(q)
    if match:
        days = int(match.group(1))
        end = now
        start = now - timedelta(days=days)
        return start, end, days, None

    match = _NEXT_N_DAYS.search(q)
    if match:
        days = int(match.group(2))
        start = now
        end = now + timedelta(days=days)
        return start, end, None, days

    return None, None, None, None


def parse_topn(q: str, default: int = 10) -> int:
    match = _TOP_N.search(q)
    if match:
        return max(1, int(match.group(1)))
    return default


def is_stakeholder_rank(q: str) -> bool:
    return bool(_STAKEHOLDER.search(q)) and bool(re.search(r"\bby\b|\bvalue\b|\brank|\btop", q, re.I))


def is_department_rank(q: str) -> bool:
    return bool(_DEPARTMENT.search(q)) and bool(re.search(r"\bby\b|\bvalue\b|\brank|\btop", q, re.I))


def is_expiry_window(q: str) -> bool:
    return bool(_EXPIRE.search(q)) and bool(re.search(r"30|60|90|day", q, re.I))


def is_status_breakdown(q: str) -> bool:
    return bool(_STATUS.search(q)) and bool(re.search(r"\bcount|\bbreakdown|\bby", q, re.I))
