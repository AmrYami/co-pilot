"""Date window helper routines shared across DW rate handlers."""

from __future__ import annotations

from datetime import date, timedelta
from dateutil.relativedelta import relativedelta

_QUARTER_START_MONTH = {1: 1, 2: 4, 3: 7, 4: 10}


def _q_bounds(dt: date, delta_quarters: int = 0) -> tuple[date, date]:
    """Return the (start, end) bounds for the quarter containing ``dt``."""

    # quarter index 1..4
    q = ((dt.month - 1) // 3) + 1
    q += delta_quarters
    y = dt.year + (q - 1) // 4
    q = ((q - 1) % 4) + 1
    start = date(y, _QUARTER_START_MONTH[q], 1)
    end = (start + relativedelta(months=3)) - timedelta(days=1)
    return start, end


def last_n(unit: str, n: int, today: date) -> tuple[date, date]:
    """Return ``(start, end)`` for the last ``n`` units ending at ``today``."""

    if unit == "days":
        return today - timedelta(days=n), today
    if unit == "weeks":
        return today - timedelta(weeks=n), today
    if unit == "months":
        return today + relativedelta(months=-n), today
    if unit == "quarters":
        # n quarters back from today
        start, _ = _q_bounds(today, delta_quarters=-n)
        _, end = _q_bounds(today, delta_quarters=0)
        return start, end
    if unit == "years":
        return today + relativedelta(years=-n), today
    raise ValueError(unit)


def next_n(unit: str, n: int, today: date) -> tuple[date, date]:
    """Return ``(start, end)`` for the next ``n`` units starting at ``today``."""

    if unit == "days":
        return today, today + timedelta(days=n)
    if unit == "weeks":
        return today, today + timedelta(weeks=n)
    if unit == "months":
        return today, today + relativedelta(months=+n)
    if unit == "quarters":
        start, end = _q_bounds(today, delta_quarters=+n)
        # لو عايز "الربع القادم" فقط: استخدم حدود الربع القادم حرفيًا
        return start, end
    if unit == "years":
        return today, today + relativedelta(years=+n)
    raise ValueError(unit)


def this_quarter(today: date) -> tuple[date, date]:
    """Return the quarter bounds for ``today``."""

    return _q_bounds(today)


def between(d1: date, d2: date) -> tuple[date, date]:
    """Return a normalized ``(start, end)`` tuple."""

    return (min(d1, d2), max(d1, d2))


__all__ = [
    "between",
    "last_n",
    "next_n",
    "this_quarter",
]

