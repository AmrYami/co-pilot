"""Rule-based normaliser for DocuWare natural language questions.

This module focuses on extracting a deterministic intent object from loose
English/Arabic questions.  It covers basic aggregation detection, time window
normalisation (relative ranges, explicit quarters, etc.), grouping synonyms and
simple measure selection (gross vs net value).
"""

from __future__ import annotations

from calendar import monthrange
from datetime import datetime, timedelta, timezone
import re
from typing import Any, Dict, Optional, Tuple

try:  # pragma: no cover - optional dependency
    # Optional, improves parsing like "last 3 months", "next week"
    import dateparser  # type: ignore  # pylint: disable=import-error
except Exception:  # pragma: no cover - fallback when dateparser not installed
    dateparser = None

# ---- Config knobs you can later move to mem_settings ----

DEFAULT_TZ = timezone(timedelta(hours=3))  # Africa/Cairo (+03:00 currently)
DEFAULT_DATE_COL = "REQUEST_DATE"  # DW default
DIMENSION_SYNONYMS = {
    # EN
    r"\bstakeholders?\b": "CONTRACT_STAKEHOLDER_1",
    r"\bowner dept(?:artment)?\b": "OWNER_DEPARTMENT",
    r"\bdepartment\b": "OWNER_DEPARTMENT",
    r"\bentity\b": "ENTITY_NO",
    r"\bowner\b": "CONTRACT_OWNER",
    # AR (basic)
    r"\bصاحب العقد\b": "CONTRACT_OWNER",
    r"\bالقسم\b": "OWNER_DEPARTMENT",
    r"\bجه(?:ة|ات)\b": "ENTITY_NO",
    r"\bأصحاب المصلحة\b": "CONTRACT_STAKEHOLDER_1",
}

GROSS_VALUE_EXPR = (
    "NVL(CONTRACT_VALUE_NET_OF_VAT,0) + CASE WHEN NVL(VAT,0) BETWEEN 0 AND 1 "
    "THEN NVL(CONTRACT_VALUE_NET_OF_VAT,0) * NVL(VAT,0) ELSE NVL(VAT,0) END"
)
NET_VALUE_EXPR = "NVL(CONTRACT_VALUE_NET_OF_VAT,0)"


# ---- Datamodel ----

from core.nlu.schema import NLIntent, TimeWindow


# ---- Helpers ----


_NUM_WORDS = {
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
    "thirteen": 13,
    "fourteen": 14,
    "fifteen": 15,
    "sixteen": 16,
    "seventeen": 17,
    "eighteen": 18,
    "nineteen": 19,
    "twenty": 20,
}
_AR_NUM_WORDS = {
    "واحد": 1,
    "اثنين": 2,
    "اتنين": 2,
    "ثلاثة": 3,
    "اربعة": 4,
    "أربعة": 4,
    "خمسة": 5,
    "ستة": 6,
    "سبعة": 7,
    "ثمانية": 8,
    "تسعة": 9,
    "عشرة": 10,
    "عشرون": 20,
}


def _to_int(tok: str) -> Optional[int]:
    t = tok.lower()
    if t.isdigit():
        return int(t)
    return _NUM_WORDS.get(t) or _AR_NUM_WORDS.get(t)


def _ensure_window(intent: NLIntent) -> TimeWindow:
    if intent.explicit_dates is None:
        intent.explicit_dates = TimeWindow()
    return intent.explicit_dates


def _set_window(intent: NLIntent, *, start: Optional[str] = None, end: Optional[str] = None) -> None:
    window = _ensure_window(intent)
    if start is not None:
        window.start = start
    if end is not None:
        window.end = end
    if window.start and window.end:
        intent.has_time_window = True


def _month_range(dt: datetime) -> Tuple[datetime, datetime]:
    start = dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if start.month == 12:
        nextm = start.replace(year=start.year + 1, month=1)
    else:
        nextm = start.replace(month=start.month + 1)
    end = nextm - timedelta(seconds=1)
    return start, end


def _shift_month(dt: datetime, months: int) -> datetime:
    year = dt.year + (dt.month - 1 + months) // 12
    month = (dt.month - 1 + months) % 12 + 1
    day = min(dt.day, monthrange(year, month)[1])
    return dt.replace(year=year, month=month, day=day)


def _last_quarter_range(now: datetime) -> Tuple[datetime, datetime]:
    q = (now.month - 1) // 3 + 1
    q_start_month = 3 * (q - 1) + 1
    start_this_q = now.replace(
        month=q_start_month, day=1, hour=0, minute=0, second=0, microsecond=0
    )
    m = q_start_month - 3
    y = start_this_q.year + (m <= 0)
    m = m if m > 0 else m + 12
    start_last_q = start_this_q.replace(year=y, month=m)
    end_last_q = start_this_q - timedelta(seconds=1)
    return start_last_q, end_last_q


def _quarter_range(year: int, quarter: int, tz: timezone) -> Tuple[datetime, datetime]:
    quarter = max(1, min(quarter, 4))
    start_month = 3 * (quarter - 1) + 1
    start = datetime(year, start_month, 1, tzinfo=tz)
    if quarter == 4:
        next_q = datetime(year + 1, 1, 1, tzinfo=tz)
    else:
        next_q = datetime(year, start_month + 3, 1, tzinfo=tz)
    end = next_q - timedelta(seconds=1)
    return start, end


def _year_range(year: int, tz: timezone) -> Tuple[datetime, datetime]:
    start = datetime(year, 1, 1, tzinfo=tz)
    end = datetime(year + 1, 1, 1, tzinfo=tz) - timedelta(seconds=1)
    return start, end


# ---- Core normalizer ----


def normalize(question: str, now: Optional[datetime] = None) -> NLIntent:
    now = now or datetime.now(tz=DEFAULT_TZ)
    q = (question or "").strip()
    intent = NLIntent(wants_all_columns=("select" not in q.lower()), notes={})

    # 1) Aggregation
    if re.search(r"\bcount\b|\(count\)", q, re.I) or re.search(r"\bعدد\b", q):
        intent.agg = "count"
    elif re.search(r"\b(sum|total|اجمالي|إجمالي)\b", q, re.I):
        intent.agg = "sum"
    elif re.search(r"\b(avg|average|متوسط)\b", q, re.I):
        intent.agg = "avg"
    elif re.search(r"\bmin(imum)?\b|أقل", q, re.I):
        intent.agg = "min"
    elif re.search(r"\bmax(imum)?\b|أعلى|اكبر|أكبر", q, re.I):
        intent.agg = "max"

    # 2) Measure: gross vs net
    if re.search(r"\bgross\b|إجمالي شامل|شامل الضريبة", q, re.I):
        intent.measure_sql = GROSS_VALUE_EXPR
    elif re.search(r"\bnet\b|صافي", q, re.I):
        intent.measure_sql = NET_VALUE_EXPR
    elif re.search(r"\b(contract value|value)\b|قيمة العقد", q, re.I):
        intent.measure_sql = NET_VALUE_EXPR  # default for “contract value”

    # 3) Grouping (by/per <dimension>)
    m = re.search(r"\b(?:by|per)\s+([A-Za-z_ ]+)\b", q, re.I)
    if m:
        dim_txt = m.group(1).strip().lower()
        for pat, col in DIMENSION_SYNONYMS.items():
            if re.search(pat, dim_txt, re.I):
                intent.group_by = col
                break
    # Arabic: "حسب <dimension>"
    m = re.search(r"حسب\s+([^\s]+)", q)
    if not intent.group_by and m:
        dim_txt = m.group(1).strip()
        for pat, col in DIMENSION_SYNONYMS.items():
            if re.search(pat, dim_txt, re.I):
                intent.group_by = col
                break
    if not intent.group_by:
        for pat, col in DIMENSION_SYNONYMS.items():
            if re.search(pat, q, re.I):
                intent.group_by = col
                break

    # 4) Top/Bottom N
    tb = re.search(
        r"\b(top|highest|best|الأعلى|افضل|أفضل|أكبر|الأكثر)\s+(\w+)\b", q, re.I
    )
    if tb:
        n = _to_int(tb.group(2))
        if n:
            intent.top_n = n
            intent.user_requested_top_n = True
            intent.sort_desc = True
    bb = re.search(r"\b(bottom|lowest|least|الأقل|أصغر|الاقل)\s+(\w+)\b", q, re.I)
    if bb:
        n = _to_int(bb.group(2))
        if n:
            intent.top_n = n
            intent.user_requested_top_n = True
            intent.sort_desc = False

    # 5) Time window
    ql = q.lower()
    tz = now.tzinfo or DEFAULT_TZ
    # Explicit phrases
    if "last month" in ql or "الشهر الماضي" in ql:
        start, end = _month_range(now.replace(day=1) - timedelta(days=1))
        _set_window(intent, start=start.date().isoformat(), end=end.date().isoformat())
    elif "next month" in ql or "الشهر القادم" in ql:
        probe = now.replace(day=28) + timedelta(days=4)
        start, end = _month_range(probe)
        _set_window(intent, start=start.date().isoformat(), end=end.date().isoformat())
    elif "this month" in ql or "هذا الشهر" in ql:
        start, end = _month_range(now)
        _set_window(intent, start=start.date().isoformat(), end=end.date().isoformat())
    elif "last quarter" in ql or "الربع الماضي" in ql:
        start, end = _last_quarter_range(now)
        _set_window(intent, start=start.date().isoformat(), end=end.date().isoformat())
    elif "this quarter" in ql or "الربع الحالي" in ql:
        current_q = (now.month - 1) // 3 + 1
        start, end = _quarter_range(now.year, current_q, tz)
        _set_window(intent, start=start.date().isoformat(), end=end.date().isoformat())
    elif "next quarter" in ql or "الربع القادم" in ql:
        current_q = (now.month - 1) // 3 + 1
        next_q = current_q + 1
        year = now.year
        if next_q > 4:
            next_q = 1
            year += 1
        start, end = _quarter_range(year, next_q, tz)
        _set_window(intent, start=start.date().isoformat(), end=end.date().isoformat())
    elif "last year" in ql or "السنة الماضية" in ql or "العام الماضي" in ql:
        start, end = _year_range(now.year - 1, tz)
        _set_window(intent, start=start.date().isoformat(), end=end.date().isoformat())
    elif "this year" in ql or "هذه السنة" in ql or "هذا العام" in ql or "العام الحالي" in ql:
        start, end = _year_range(now.year, tz)
        _set_window(intent, start=start.date().isoformat(), end=end.date().isoformat())
    elif "next year" in ql or "السنة القادمة" in ql or "العام القادم" in ql:
        start, end = _year_range(now.year + 1, tz)
        _set_window(intent, start=start.date().isoformat(), end=end.date().isoformat())
    else:
        q_match = re.search(r"\bq([1-4])\s*(?:/|\-)?\s*(\d{4})\b", q, re.I)
        if q_match:
            quarter = int(q_match.group(1))
            year = int(q_match.group(2))
            start, end = _quarter_range(year, quarter, tz)
            _set_window(intent, start=start.date().isoformat(), end=end.date().isoformat())
        else:
            n_window = re.search(
                r"\b(last|next|within|in|القادم|الماضي|السابقة)\s+(\d+|\w+)\s+"
                r"(day|days|week|weeks|month|months|year|years|يوم|أيام|اسبوع|أسابيع|شهر|شهور|سنة|سنوات)\b",
                q,
                re.I,
            )
            if n_window:
                dir_word, num_tok, unit = n_window.groups()
                n = _to_int(num_tok) or 0
                if n > 0:
                    direction = "last" if re.search(r"last|الماضي|السابقة", dir_word, re.I) else "next"
                    if re.search(r"month|شهر|شهور", unit, re.I):
                        if direction == "last":
                            start_probe = _shift_month(now, -n)
                            start = start_probe.replace(hour=0, minute=0, second=0, microsecond=0, day=1)
                            _set_window(intent, start=start.date().isoformat(), end=now.date().isoformat())
                        else:
                            end_probe = _shift_month(now, n)
                            end = _month_range(end_probe)[1]
                            _set_window(intent, start=now.date().isoformat(), end=end.date().isoformat())
                    else:
                        scale = 1
                        if re.search(r"week|اسبوع|أسابيع", unit, re.I):
                            scale = 7
                        if re.search(r"year|سنة|سنوات", unit, re.I):
                            scale = 365
                        delta = timedelta(days=scale * n)
                        if direction == "last":
                            _set_window(intent, start=(now - delta).date().isoformat(), end=now.date().isoformat())
                        else:
                            _set_window(intent, start=now.date().isoformat(), end=(now + delta).date().isoformat())

            if (
                dateparser
                and (
                    not intent.explicit_dates
                    or not intent.explicit_dates.start
                    or not intent.explicit_dates.end
                )
            ):
                m = re.search(r"\bbetween\s+(.+?)\s+and\s+(.+)$", ql, re.I) or re.search(
                    r"\bfrom\s+(.+?)\s+(?:to|-)\s+(.+)$", ql, re.I
                )
                if m:
                    ds = dateparser.parse(m.group(1), settings={"RELATIVE_BASE": now})
                    de = dateparser.parse(m.group(2), settings={"RELATIVE_BASE": now})
                    if ds and de:
                        _set_window(intent, start=ds.date().isoformat(), end=de.date().isoformat())

    # Choose date column if user mentions END_DATE/START_DATE explicitly or via keywords
    if re.search(r"\bEND_DATE\b", q, re.I) or "تاريخ الانتهاء" in q or re.search(r"expir", q, re.I):
        intent.date_column = "END_DATE"
    elif re.search(r"\bSTART_DATE\b", q, re.I) or "تاريخ البداية" in q:
        intent.date_column = "START_DATE"
    else:
        intent.date_column = DEFAULT_DATE_COL

    # Sorting heuristic: if user asked “top/bottom” → sort by measure
    if intent.top_n and not intent.sort_by:
        intent.sort_by = intent.measure_sql or NET_VALUE_EXPR

    # If user said “by <dimension>” and asked for “top N … by <dim>”
    # we sort by the measure of the aggregation
    if intent.group_by and not intent.sort_by:
        intent.sort_by = intent.measure_sql or NET_VALUE_EXPR

    if intent.agg:
        intent.wants_all_columns = False

    if intent.notes is None:
        intent.notes = {}

    return intent


__all__ = [
    "DEFAULT_TZ",
    "DEFAULT_DATE_COL",
    "DIMENSION_SYNONYMS",
    "GROSS_VALUE_EXPR",
    "NET_VALUE_EXPR",
    "NLIntent",
    "normalize",
]

