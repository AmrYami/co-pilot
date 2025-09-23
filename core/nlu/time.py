from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
import re

from calendar import monthrange

try:  # pragma: no cover - optional dependency
    import dateparser  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - allow running without dependency
    dateparser = None


_Q_LAST = re.compile(r"\blast\s+quarter\b", re.I)
_Q_YTD = re.compile(r"\bYTD\b|\byear\s*to\s*date\b", re.I)
_Q_LAST_N = re.compile(
    r"\blast\s+(\d+)\s+(day|days|week|weeks|month|months|quarter|quarters|year|years)\b",
    re.I,
)
_Q_NEXT_N = re.compile(
    r"\bnext\s+(\d+)\s+(day|days|week|weeks|month|months|quarter|quarters|year|years)\b",
    re.I,
)


@dataclass
class Window:
    start: str
    end: str


def iso(d: date) -> str:
    return d.isoformat()


def month_bounds(dt: date) -> tuple[date, date]:
    start = dt.replace(day=1)
    end = _shift_month(start, 1) - timedelta(days=1)
    return start, end


def quarter_bounds(dt: date) -> tuple[date, date]:
    q = (dt.month - 1) // 3
    start = date(dt.year, q * 3 + 1, 1)
    end = _shift_month(start, 3) - timedelta(days=1)
    return start, end


def _shift_month(dt: date, months: int) -> date:
    year = dt.year + (dt.month - 1 + months) // 12
    month = (dt.month - 1 + months) % 12 + 1
    day = min(dt.day, monthrange(year, month)[1])
    return date(year, month, day)


def _shift_year(dt: date, years: int) -> date:
    try:
        return dt.replace(year=dt.year + years)
    except ValueError:
        return date(dt.year + years, 2, 28)


def resolve_window(text: str, now: date | None = None) -> Window | None:
    """Return [start,end] ISO when we can; else None."""

    t = text or ""
    today = now or date.today()

    if _Q_LAST.search(t):
        cur_q_start, _ = quarter_bounds(today)
        last_q_end = cur_q_start - timedelta(days=1)
        last_q_start, last_q_end2 = quarter_bounds(last_q_end)
        return Window(iso(last_q_start), iso(last_q_end2))

    if _Q_YTD.search(t):
        start = date(today.year, 1, 1)
        return Window(iso(start), iso(today))

    for rx, sign in ((_Q_LAST_N, -1), (_Q_NEXT_N, +1)):
        m = rx.search(t)
        if m:
            n = int(m.group(1))
            unit = m.group(2).lower()
            if unit.startswith("day"):
                delta_days = n
                if sign < 0:
                    start = today - timedelta(days=delta_days)
                    end = today
                else:
                    start = today
                    end = today + timedelta(days=delta_days)
            elif unit.startswith("week"):
                delta_days = 7 * n
                if sign < 0:
                    start = today - timedelta(days=delta_days)
                    end = today
                else:
                    start = today
                    end = today + timedelta(days=delta_days)
            elif unit.startswith("month"):
                if sign < 0:
                    start = _shift_month(today, -n)
                    end = today
                else:
                    start = today
                    end = _shift_month(today, n)
            elif unit.startswith("quarter"):
                months = 3 * n
                if sign < 0:
                    start = _shift_month(today, -months)
                    end = today
                else:
                    start = today
                    end = _shift_month(today, months)
            else:
                if sign < 0:
                    start = _shift_year(today, -n)
                    end = today
                else:
                    start = today
                    end = _shift_year(today, n)
            return Window(iso(start), iso(end))

    parsed = None
    if dateparser and hasattr(dateparser, "search"):
        parsed = dateparser.search.search_dates(
            t, settings={"RETURN_AS_TIMEZONE_AWARE": False}
        )
    if parsed and len(parsed) >= 1:
        dates = [d for _, d in parsed]
        if len(dates) >= 2:
            ds, de = sorted([dates[0].date(), dates[1].date()])
            return Window(iso(ds), iso(de))

    lower = t.lower()
    if "last month" in lower:
        prev_month_end = today.replace(day=1) - timedelta(days=1)
        prev_month_start = prev_month_end.replace(day=1)
        return Window(iso(prev_month_start), iso(prev_month_end))

    if "next month" in lower:
        nm_start = _shift_month(today.replace(day=1), 1)
        nm_end = _shift_month(nm_start, 1) - timedelta(days=1)
        return Window(iso(nm_start), iso(nm_end))

    return None
