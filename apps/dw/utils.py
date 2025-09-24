from __future__ import annotations

import os
import re
from calendar import monthrange
from datetime import datetime, timedelta, timezone

try:  # pragma: no cover - optional dependency in tests
    from dateutil.relativedelta import relativedelta
except Exception:  # pragma: no cover - fallback when python-dateutil missing
    relativedelta = None  # type: ignore[assignment]


def env_flag(name: str, default: bool = False) -> bool:
    v = (os.getenv(name, "1" if default else "0") or "").strip().lower()
    return v in ("1", "true", "yes", "y", "on")


def env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


def today_utc() -> datetime:
    return datetime.now(timezone.utc)


def last_n_days(n: int, ref: datetime | None = None) -> tuple[str, str]:
    ref = ref or today_utc()
    start = ref - timedelta(days=n)
    return start.date().isoformat(), ref.date().isoformat()


def last_n_months(n: int, ref: datetime | None = None) -> tuple[str, str]:
    ref = ref or today_utc()
    if relativedelta is not None:
        start_dt = ref - relativedelta(months=n)
    else:  # pragma: no cover - simple fallback
        month_index = ref.month - 1 - n
        year = ref.year + month_index // 12
        month = (month_index % 12) + 1
        day = min(ref.day, monthrange(year, month)[1])
        start_dt = ref.replace(year=year, month=month, day=day)
    start = start_dt.date().isoformat()
    return start, ref.date().isoformat()


def last_month(ref: datetime | None = None) -> tuple[str, str]:
    ref = ref or today_utc()
    first_this = ref.replace(day=1).date()
    last_prev = first_this - timedelta(days=1)
    first_prev = last_prev.replace(day=1)
    return first_prev.isoformat(), last_prev.isoformat()


REQUEST_SYNONYMS = re.compile(r"\b(request|requested|request date|طلب)\b", re.I)


def mentions_requested(text: str) -> bool:
    return bool(REQUEST_SYNONYMS.search(text or ""))
