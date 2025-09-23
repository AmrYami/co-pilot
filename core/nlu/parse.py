from __future__ import annotations

import re

from core.dates import parse_time_window
from core.nlu.schema import NLIntent, TimeWindow

DIM_SYNONYMS = {
    r"\bstakeholder(s)?\b|المساهم|صاحب المصلحة": "CONTRACT_STAKEHOLDER_1",
    r"\b(owner\s+department|department)\b|القسم|الإدارة": "OWNER_DEPARTMENT",
    r"\bowner\b|المالك": "CONTRACT_OWNER",
    r"\bentity\b|الكيان": "ENTITY_NO",
}

_RE_TOP = re.compile(
    r"\btop\s+(\d+|[A-Za-z]+)\b|أعلى\s+(\d+|[^\s]+)",
    re.I,
)
_RE_COUNT = re.compile(r"\bcount\b|\(count\)", re.I)
_RE_GROSS = re.compile(r"\bgross\b|إجمالي", re.I)
_RE_NET = re.compile(r"\bnet\b|صافي", re.I)
_RE_BY = re.compile(r"\bby\s+([a-z_ ]+)\b|\bper\s+([a-z_ ]+)\b|حسب\s+([^\s]+)", re.I)

_TOP_WORDS = {
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
    "إحدى": 11,
    "إحدى عشر": 11,
    "احدى عشر": 11,
    "اثنا عشر": 12,
    "twenty": 20,
    "عشرين": 20,
}


def _map_dimension(text: str) -> str | None:
    for pat, col in DIM_SYNONYMS.items():
        if re.search(pat, text, re.I):
            return col
    return None


def _top_value(token: str | None) -> int | None:
    if not token:
        return None
    token = token.strip()
    if token.isdigit():
        return int(token)
    lowered = token.lower()
    if lowered in _TOP_WORDS:
        return _TOP_WORDS[lowered]
    return _TOP_WORDS.get(token)


def parse_intent(
    question: str,
    default_date_col: str = "REQUEST_DATE",
    select_all_default: bool = True,
) -> NLIntent:
    text = question or ""
    lowered = text.lower()
    intent = NLIntent()
    intent.notes["q"] = text

    date_col = (default_date_col or "REQUEST_DATE").strip().upper() or "REQUEST_DATE"
    intent.date_column = date_col

    start, end = parse_time_window(text)
    if start and end:
        intent.has_time_window = True
        intent.explicit_dates = TimeWindow(start=start.isoformat(), end=end.isoformat())
    else:
        intent.has_time_window = None

    if m := _RE_TOP.search(text):
        raw = m.group(1) or m.group(2) or m.group(3)
        n = _top_value(raw) or 10
        intent.top_n = n
        intent.user_requested_top_n = True
        intent.sort_desc = True

    if m := _RE_BY.search(text):
        dim_phrase = next((g for g in m.groups() if g), "")
        intent.group_by = _map_dimension(dim_phrase) or _map_dimension(text)
    else:
        intent.group_by = _map_dimension(text)

    if _RE_COUNT.search(text):
        intent.agg = "count"

    if _RE_GROSS.search(text):
        intent.measure_sql = (
            "NVL(CONTRACT_VALUE_NET_OF_VAT,0) + CASE WHEN NVL(VAT,0) BETWEEN 0 AND 1 "
            "THEN NVL(CONTRACT_VALUE_NET_OF_VAT,0)*NVL(VAT,0) ELSE NVL(VAT,0) END"
        )
    elif _RE_NET.search(text) or "contract value" in lowered:
        intent.measure_sql = "NVL(CONTRACT_VALUE_NET_OF_VAT,0)"

    if intent.top_n:
        if intent.group_by:
            intent.sort_by = intent.measure_sql or "NVL(CONTRACT_VALUE_NET_OF_VAT,0)"
        else:
            intent.sort_by = intent.date_column or date_col
        intent.sort_desc = True

    if not intent.measure_sql:
        intent.measure_sql = "NVL(CONTRACT_VALUE_NET_OF_VAT,0)"

    wants_all = bool(select_all_default)
    if intent.group_by or intent.agg:
        wants_all = False
    intent.wants_all_columns = wants_all

    return intent
