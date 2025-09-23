from __future__ import annotations
from calendar import monthrange
from datetime import date, datetime, timedelta
import re

_NUM = r"(?:\d+|one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|ثلاثة|ثلاث|اثنين|اثنان|واحد|خمسة|ستة|سبعة|ثمانية|تسعة|عشرة|إحدى عشر|اثنا عشر)"


def _to_int(s: str) -> int:
    m = re.match(r"\d+$", s)
    if m:
        return int(s)
    eng = {
        "one": 1,
        "two": 2,
        "three": 3,
        "four": 4,
        "five": 5,
        "six": 6,
        "seven": 7,
        "eight": 8,
        "nine": 9,
        "ten": 10,
        "eleven": 11,
        "twelve": 12,
    }
    ar = {
        "واحد": 1,
        "اثنين": 2,
        "اثنان": 2,
        "ثلاث": 3,
        "ثلاثة": 3,
        "أربع": 4,
        "أربعة": 4,
        "خمسة": 5,
        "ستة": 6,
        "سبعة": 7,
        "ثمانية": 8,
        "تسعة": 9,
        "عشرة": 10,
        "إحدى عشر": 11,
        "اثنا عشر": 12,
    }
    return eng.get(s.lower(), ar.get(s, 1))


def last_month_window(today: date | None = None) -> tuple[date, date]:
    d = today or date.today()
    first_this = d.replace(day=1)
    last_prev = first_this - timedelta(days=1)
    first_prev = last_prev.replace(day=1)
    return first_prev, last_prev


def last_quarter_window(today: date | None = None) -> tuple[date, date]:
    d = today or date.today()
    q = (d.month - 1) // 3 + 1
    pq_last_month = (q - 2) * 3 + 3
    year = d.year if q > 1 else d.year - 1
    start = date(year, pq_last_month - 2, 1)
    first_next = _add_months(start, 3)
    end = first_next - timedelta(days=1)
    return start, end


def parse_time_window(text: str, today: date | None = None) -> tuple[date | None, date | None]:
    """Return (start_date, end_date) or (None, None) if not found."""
    t = (text or "").lower()
    d = today or date.today()

    if "last quarter" in t or "الربع الماضي" in t:
        return last_quarter_window(d)
    if "last month" in t or "الشهر الماضي" in t:
        return last_month_window(d)
    if "yesterday" in t or "أمس" in t:
        y = d - timedelta(days=1)
        return y, y
    if "today" in t or "اليوم" in t:
        return d, d
    if m := re.search(r"next\s+(\d+)\s+days", t):
        n = int(m.group(1))
        return d, d + timedelta(days=n)
    if m := re.search(r"last\s+(\d+)\s+days", t):
        n = int(m.group(1))
        return d - timedelta(days=n), d
    if m := re.search(r"last\s+(" + _NUM + r")\s+months?", t):
        n = _to_int(m.group(1))
        return _add_months(d, -n), d
    if m := re.search(r"last\s+(" + _NUM + r")\s+weeks?", t):
        n = _to_int(m.group(1))
        return d - timedelta(weeks=n), d
    if "last 3 months" in t or "آخر 3 شهور" in t:
        return _add_months(d, -3), d

    return None, None


def _add_months(d: date, months: int) -> date:
    year = d.year + (d.month - 1 + months) // 12
    month = (d.month - 1 + months) % 12 + 1
    day = min(d.day, monthrange(year, month)[1])
    return date(year, month, day)
