from __future__ import annotations

import re
from datetime import datetime, timedelta
from typing import Tuple

from dateutil.relativedelta import relativedelta

try:  # pragma: no cover - optional dependency during tests
    from word2number.w2n import word_to_num
except Exception:  # pragma: no cover - fallback when package missing
    word_to_num = None  # type: ignore[assignment]

from .models import NLIntent
from .table_profiles import DIM_SYNONYMS, gross_sql, net_sql

_RE_TOP = re.compile(r"\btop\s+(\d+|\w+)\b", re.I)
_RE_LAST_N_MONTHS = re.compile(r"\blast\s+(\d+|\w+)\s+months?\b", re.I)
_RE_LAST_MONTH = re.compile(r"\blast\s+month\b", re.I)
_RE_LAST_N_DAYS = re.compile(r"\b(?:next|in)\s+(\d+|\w+)\s+days?\b", re.I)
_RE_EXPIRE = re.compile(r"\bexpir\w*\b", re.I)
_RE_REQUESTED = re.compile(r"\brequested?\b", re.I)
_RE_GROSS = re.compile(r"\bgross\b", re.I)
_RE_AVG = re.compile(r"\baverage|avg\b", re.I)
_RE_COUNT = re.compile(r"\bcount\b|\(count\)", re.I)
_RE_BY = re.compile(r"\bby\s+([a-zA-Z_ ]+)\b", re.I)
_RE_YEAR = re.compile(r"\b(20\d{2})\b")
_RE_YTD = re.compile(r"\bYTD\b", re.I)
_RE_YTD_EXPLICIT = re.compile(r"\b(20\d{2})\s*(?:ytd|year\s*to\s*date)\b", re.I)
_RE_LAST_12 = re.compile(r"\blast\s+12\s+months?\b", re.I)
_RE_LAST_90D = re.compile(r"\blast\s+90\s+days?\b", re.I)


def _to_int(token: str) -> int:
    try:
        return int(token)
    except Exception:
        if word_to_num is None:
            return 0
        try:
            return int(word_to_num(token))
        except Exception:
            return 0


def _month_window(n: int) -> Tuple[str, str]:
    end = datetime.utcnow().date()
    start = (end - relativedelta(months=n)).replace(day=1)
    return start.isoformat(), end.isoformat()


def _last_month_window() -> Tuple[str, str]:
    today = datetime.utcnow().date().replace(day=1)
    last_start = today - relativedelta(months=1)
    last_end = today - relativedelta(days=1)
    return last_start.isoformat(), last_end.isoformat()


def _next_days(n: int) -> Tuple[str, str]:
    start = datetime.utcnow().date()
    end = start + timedelta(days=n)
    return start.isoformat(), end.isoformat()


def parse_intent(question: str) -> NLIntent:
    text = (question or "").strip()
    intent = NLIntent(question=text)
    intent.wants_all_columns = True

    # Top N handling
    m = _RE_TOP.search(text)
    if m:
        n = _to_int(m.group(1))
        if n > 0:
            intent.top_n = n
            intent.user_requested_top_n = True

    # Time windows
    if _RE_LAST_MONTH.search(text):
        start, end = _last_month_window()
        intent.has_time_window = True
        intent.explicit_dates = {"start": start, "end": end}
    else:
        m = _RE_LAST_N_MONTHS.search(text)
        if m:
            n = max(_to_int(m.group(1)), 1)
            start, end = _month_window(n)
            intent.has_time_window = True
            intent.explicit_dates = {"start": start, "end": end}
        elif _RE_LAST_12.search(text):
            start, end = _month_window(12)
            intent.has_time_window = True
            intent.explicit_dates = {"start": start, "end": end}
        elif _RE_LAST_90D.search(text):
            start, end = _next_days(-90)
            intent.has_time_window = True
            intent.explicit_dates = {"start": start, "end": end}

    # Expiring window
    if _RE_EXPIRE.search(text) or "expiring" in text.lower():
        intent.expire = True
        n_days = 30
        m = _RE_LAST_N_DAYS.search(text)
        if m:
            parsed = _to_int(m.group(1))
            if parsed > 0:
                n_days = parsed
        start, end = _next_days(n_days)
        intent.has_time_window = True
        intent.explicit_dates = {"start": start, "end": end}

    # Default date column logic
    if _RE_REQUESTED.search(text):
        intent.date_column = "REQUEST_DATE"
    else:
        intent.date_column = "OVERLAP"

    # Aggregations
    if _RE_COUNT.search(text):
        intent.agg = "count"
        intent.wants_all_columns = False
    elif _RE_AVG.search(text):
        intent.agg = "avg"
        intent.wants_all_columns = False

    # Measure selection
    if _RE_GROSS.search(text):
        intent.measure_sql = gross_sql()
    else:
        intent.measure_sql = net_sql()

    # Grouping via "by" phrase
    m = _RE_BY.search(text)
    if m:
        raw_dim = m.group(1).strip().lower()
        intent.group_by = DIM_SYNONYMS.get(raw_dim)
        if intent.group_by:
            intent.wants_all_columns = False

    lowered = text.lower()

    if "stakeholder" in lowered:
        pass
    if "status" in lowered and intent.agg in (None, "count"):
        intent.group_by = "CONTRACT_STATUS"
        intent.agg = intent.agg or "count"
        intent.wants_all_columns = False

    if "distinct entity" in lowered or "entity values" in lowered:
        intent.group_by = "ENTITY"
        intent.agg = "count"
        intent.wants_all_columns = False

    if "missing contract_id" in lowered:
        intent.filters["MISSING_ID"] = "1"
        intent.wants_all_columns = True

    if "vat" in lowered and (("null" in lowered or "zero" in lowered) and ("> 0" in lowered or "greater than 0" in lowered)):
        intent.filters["VAT_ZERO_AND_VALUE_POS"] = "1"

    if "monthly trend" in lowered:
        intent.filters["MONTHLY_TREND"] = "1"
        intent.wants_all_columns = False
        intent.agg = "count"
        intent.group_by = "REQUEST_DATE"
        if not intent.explicit_dates:
            end = datetime.utcnow().date()
            start = (end - relativedelta(months=12)).replace(day=1)
            intent.has_time_window = True
            intent.explicit_dates = {"start": start.isoformat(), "end": end.isoformat()}

    m_ytd = _RE_YTD_EXPLICIT.search(text)
    if m_ytd:
        year = int(m_ytd.group(1))
        today = datetime.utcnow().date()
        start = f"{year}-01-01"
        end = today.isoformat() if year == today.year else f"{year}-12-31"
        intent.has_time_window = True
        intent.explicit_dates = {"start": start, "end": end}
        if not intent.date_column or intent.date_column == "OVERLAP":
            intent.date_column = "OVERLAP"
        intent.notes["window"] = "ytd"
    elif _RE_YTD.search(text):
        year = datetime.utcnow().year
        start = f"{year}-01-01"
        end = datetime.utcnow().date().isoformat()
        intent.has_time_window = True
        intent.explicit_dates = {"start": start, "end": end}
        if not intent.date_column or intent.date_column == "OVERLAP":
            intent.date_column = "OVERLAP"
        intent.notes["window"] = "ytd"

    m = _RE_YEAR.search(text)
    if m and "requested" in lowered:
        year = int(m.group(1))
        intent.explicit_dates = {"start": f"{year}-01-01", "end": f"{year}-12-31"}
        intent.has_time_window = True
        intent.date_column = "REQUEST_DATE"

    intent.sort_by = intent.measure_sql
    intent.sort_desc = True
    return intent
