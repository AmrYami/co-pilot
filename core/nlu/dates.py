"""Relative/absolute date extraction for English and Arabic text."""

from __future__ import annotations

import datetime as dt
import re
from calendar import monthrange
from typing import Tuple

try:  # pragma: no cover - optional dependency
    import dateparser
except Exception:  # pragma: no cover
    dateparser = None  # type: ignore

_AR_OR_EN = {"languages": ["en", "ar"], "RELATIVE_BASE": None}


def _today(tz: str | None) -> dt.date:
    return dt.datetime.utcnow().date()


def _fmt(d: dt.date) -> str:
    return d.strftime("%Y-%m-%d")


def _add_months(d: dt.date, months: int) -> dt.date:
    year = d.year + (d.month - 1 + months) // 12
    month = (d.month - 1 + months) % 12 + 1
    day = min(d.day, monthrange(year, month)[1])
    return dt.date(year, month, day)


def _add_years(d: dt.date, years: int) -> dt.date:
    try:
        return d.replace(year=d.year + years)
    except ValueError:
        # Handle leap day gracefully
        return d.replace(month=2, day=28, year=d.year + years)


def quarter_bounds(d: dt.date) -> tuple[dt.date, dt.date]:
    q = (d.month - 1) // 3
    start_month = 3 * q + 1
    start = dt.date(d.year, start_month, 1)
    end = _add_months(start, 3) - dt.timedelta(days=1)
    return start, end


def last_quarter_bounds(d: dt.date) -> tuple[dt.date, dt.date]:
    start, _ = quarter_bounds(d)
    start_prev = _add_months(start, -3)
    end_prev = _add_months(start_prev, 3) - dt.timedelta(days=1)
    return start_prev, end_prev


def _parse_dateexpr(expr: str):
    if dateparser is None:
        return None
    try:
        return dateparser.parse(expr, settings=_AR_OR_EN)
    except Exception:
        return None


def parse_time_window(
    text: str,
    default_col: str = "REQUEST_DATE",
    tz: str | None = "Africa/Cairo",
) -> tuple[dict, bool]:
    t = (text or "").strip().lower()
    if not t:
        return ({"start": None, "end": None, "column": default_col}, False)

    ref = _today(tz)
    inferred = False

    match = re.search(r"\bbetween\b\s+(.+?)\s+\b(and|to|و)\b\s+(.+)", t)
    if match:
        a, _, b = match.groups()
        da = _parse_dateexpr(a)
        db = _parse_dateexpr(b)
        if da and db:
            start = min(da.date(), db.date())
            end = max(da.date(), db.date())
            return (
                {"start": _fmt(start), "end": _fmt(end), "column": default_col},
                True,
            )

    match = re.search(r"last\s+(\d+)\s+(day|days|week|weeks|month|months|year|years)", t)
    if not match:
        match = re.search(r"آخر\s+(\d+)\s+(يوم|أيام|أسبوع|أسابيع|شهر|أشهر|سنة|سنوات)", t)
    if match:
        n = int(match.group(1))
        unit = match.group(2)
        if unit.startswith(("day", "يوم", "أيام")):
            start = ref - dt.timedelta(days=n)
            end = ref
        elif unit.startswith(("week", "أسبوع")):
            start = ref - dt.timedelta(days=7 * n)
            end = ref
        elif unit.startswith(("month", "شهر", "أشهر")):
            start = _add_months(ref, -n)
            end = ref
        else:
            start = _add_years(ref, -n)
            end = ref
        return ({"start": _fmt(start), "end": _fmt(end), "column": default_col}, True)

    match = re.search(r"next\s+(\d+)\s+days", t) or re.search(r"القادمة\s+(\d+)\s+يوم", t)
    if match:
        n = int(match.group(1))
        start = ref
        end = ref + dt.timedelta(days=n)
        return ({"start": _fmt(start), "end": _fmt(end), "column": default_col}, True)

    if "last month" in t or "الشهر الماضي" in t:
        first_this = ref.replace(day=1)
        end_last = first_this - dt.timedelta(days=1)
        start_last = end_last.replace(day=1)
        return ({"start": _fmt(start_last), "end": _fmt(end_last), "column": default_col}, True)

    if "this month" in t or "هذا الشهر" in t:
        start = ref.replace(day=1)
        end = _add_months(start, 1) - dt.timedelta(days=1)
        return ({"start": _fmt(start), "end": _fmt(end), "column": default_col}, True)

    if "next month" in t or "الشهر القادم" in t:
        start = _add_months(ref.replace(day=1), 1)
        end = _add_months(start, 1) - dt.timedelta(days=1)
        return ({"start": _fmt(start), "end": _fmt(end), "column": default_col}, True)

    if "last quarter" in t or "الربع الماضي" in t:
        start, end = last_quarter_bounds(ref)
        return ({"start": _fmt(start), "end": _fmt(end), "column": default_col}, True)

    if "this quarter" in t or "الربع الحالي" in t:
        start, end = quarter_bounds(ref)
        return ({"start": _fmt(start), "end": _fmt(end), "column": default_col}, True)

    if "next quarter" in t or "الربع القادم" in t:
        start_current, _ = quarter_bounds(ref)
        start_next = _add_months(start_current, 3)
        end_next = _add_months(start_next, 3) - dt.timedelta(days=1)
        return ({"start": _fmt(start_next), "end": _fmt(end_next), "column": default_col}, True)

    match = re.search(r"in the next\s+(\d+)\s+days", t) or re.search(r"خلال\s+(\d+)\s+يوماً?", t)
    if match:
        n = int(match.group(1))
        start = ref
        end = ref + dt.timedelta(days=n)
        return ({"start": _fmt(start), "end": _fmt(end), "column": default_col}, True)

    if "last week" in t or "الأسبوع الماضي" in t:
        start = ref - dt.timedelta(days=ref.weekday() + 7)
        end = start + dt.timedelta(days=6)
        return ({"start": _fmt(start), "end": _fmt(end), "column": default_col}, True)

    if "this week" in t or "هذا الأسبوع" in t:
        start = ref - dt.timedelta(days=ref.weekday())
        end = start + dt.timedelta(days=6)
        return ({"start": _fmt(start), "end": _fmt(end), "column": default_col}, True)

    if "next week" in t or "الأسبوع القادم" in t:
        start = ref + dt.timedelta(days=(7 - ref.weekday()))
        end = start + dt.timedelta(days=6)
        return ({"start": _fmt(start), "end": _fmt(end), "column": default_col}, True)

    return ({"start": None, "end": None, "column": default_col}, inferred)
