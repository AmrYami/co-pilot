"""Lightweight parser for extracting date windows from ``/dw/rate`` comments."""

from __future__ import annotations

import re
from datetime import date, datetime, timedelta
from typing import Dict, Optional, Tuple

from dateutil.relativedelta import relativedelta

from utils.dates import between, last_n, next_n, this_quarter

UNITS = {
    "day": "days",
    "days": "days",
    "week": "weeks",
    "weeks": "weeks",
    "month": "months",
    "months": "months",
    "quarter": "quarters",
    "quarters": "quarters",
    "year": "years",
    "years": "years",
}

_ARABIC_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")

_DATE_FORMATS = (
    "%Y-%m-%d",
    "%d-%m-%Y",
    "%m-%d-%Y",
    "%Y/%m/%d",
    "%d/%m/%Y",
    "%m/%d/%Y",
    "%Y.%m.%d",
    "%d.%m.%Y",
    "%m.%d.%Y",
)


def _normalise_date_token(token: str) -> str:
    cleaned = token.strip().translate(_ARABIC_DIGITS)
    cleaned = re.sub(r"\s+", "", cleaned)
    return cleaned


def parse_date(token: str) -> date:
    """Parse a single date literal supporting multiple separators and locales."""

    cleaned = _normalise_date_token(token)
    if not cleaned:
        raise ValueError("empty date token")

    # Try ISO first (allows ``YYYY-MM-DD`` as well as ``YYYYMMDD``).
    try:
        return date.fromisoformat(cleaned)
    except ValueError:
        pass

    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(cleaned, fmt).date()
        except ValueError:
            continue

    # Handle compact numbers such as 20230131.
    if re.fullmatch(r"\d{8}", cleaned):
        try:
            return datetime.strptime(cleaned, "%Y%m%d").date()
        except ValueError:
            pass

    raise ValueError(f"Unrecognized date literal: {token!r}")


def parse_time_windows(comment: str, today: date) -> Dict[str, Optional[Tuple[date, date]]]:
    """Extract requested/active/expiring windows from ``comment``."""

    tw: Dict[str, Optional[Tuple[date, date]]] = {"requested": None, "active": None, "expiring": None}
    c = (comment or "").lower()

    # requested: between
    m = re.search(r"requested:\s*between\s*([0-9\-/\.]+)\s*and\s*([0-9\-/\.]+)", c)
    if m:
        try:
            d1, d2 = parse_date(m.group(1)), parse_date(m.group(2))
        except ValueError:
            pass
        else:
            tw["requested"] = between(d1, d2)

    m = re.search(r"requested:\s*(last|next)\s+(\d+)\s+(days?|weeks?|months?|quarters?|years?)", c)
    if m:
        dir_, n, unit = m.group(1), int(m.group(2)), UNITS[m.group(3)]
        tw["requested"] = (last_n if dir_ == "last" else next_n)(unit, n, today)

    m = re.search(r"requested:\s*last\s+quarter", c)
    if m:
        start, end = this_quarter(today)
        start = start + relativedelta(months=-3)
        end = (start + relativedelta(months=+3)) - timedelta(days=1)
        tw["requested"] = (start, end)

    # active (overlap)
    m = re.search(r"active:\s*between\s*([0-9\-/\.]+)\s*and\s*([0-9\-/\.]+)", c)
    if m:
        try:
            d1, d2 = parse_date(m.group(1)), parse_date(m.group(2))
        except ValueError:
            pass
        else:
            tw["active"] = between(d1, d2)

    m = re.search(r"active:\s*(last|next)\s+(\d+)\s+(days?|weeks?|months?|quarters?|years?)", c)
    if m:
        dir_, n, unit = m.group(1), int(m.group(2)), UNITS[m.group(3)]
        tw["active"] = (last_n if dir_ == "last" else next_n)(unit, n, today)

    m = re.search(r"active:\s*(last|next)\s+quarter", c)
    if m:
        cur_start, cur_end = this_quarter(today)
        if "last" in m.group(0):
            start = cur_start + relativedelta(months=-3)
            end = cur_start - timedelta(days=1)
        else:
            start = cur_end + timedelta(days=1)
            end = cur_end + relativedelta(months=+3)
        tw["active"] = (start, end)

    # expiring (END_ONLY)
    m = re.search(r"expiring:\s*between\s*([0-9\-/\.]+)\s*and\s*([0-9\-/\.]+)", c)
    if m:
        try:
            d1, d2 = parse_date(m.group(1)), parse_date(m.group(2))
        except ValueError:
            pass
        else:
            tw["expiring"] = between(d1, d2)

    m = re.search(r"expiring:\s*(next)\s+(\d+)\s+(days?|weeks?|months?|quarters?|years?)", c)
    if m:
        n, unit = int(m.group(2)), UNITS[m.group(3)]
        tw["expiring"] = next_n(unit, n, today)

    return tw


__all__ = ["parse_time_windows", "parse_date"]

