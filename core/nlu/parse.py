from __future__ import annotations

import re

try:  # pragma: no cover - optional dependency
    from word2number import w2n  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - fallback when library missing
    w2n = None

from core.nlu.schema import NLIntent, TimeWindow
from core.nlu.time import resolve_window

_RE_TOP = re.compile(r"\b(top|highest|largest|most)\b\s*(\d+|\w+)?", re.I)
_RE_BOTTOM = re.compile(r"\b(bottom|lowest|smallest|least)\b\s*(\d+|\w+)?", re.I)
_RE_COUNT = re.compile(r"\bcount\b|\(count\)", re.I)
_RE_AVG = re.compile(r"\bavg|average|mean\b", re.I)
_RE_SUM = re.compile(r"\bsum|total\b", re.I)
_RE_MIN = re.compile(r"\bmin(imum)?\b", re.I)
_RE_MAX = re.compile(r"\bmax(imum)?\b", re.I)
_RE_BY = re.compile(r"\bby\s+([A-Za-z_ ]+)|\bper\s+([A-Za-z_ ]+)", re.I)
_RE_GROSS = re.compile(r"\bgross\b", re.I)
_RE_NET = re.compile(r"\bnet\b", re.I)

DIM_SYNONYMS = {
    "owner department": "OWNER_DEPARTMENT",
    "department": "OWNER_DEPARTMENT",
    "owner": "CONTRACT_OWNER",
    "stakeholder": "CONTRACT_STAKEHOLDER_1",
    "entity": "ENTITY_NO",
}

NUM_WORDS = {
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


def _to_int(token: str | None) -> int | None:
    if not token:
        return None
    token = token.strip()
    if token.isdigit():
        return int(token)
    if w2n is not None:
        try:
            return w2n.word_to_num(token)
        except Exception:
            return None
    return NUM_WORDS.get(token.lower())


def parse_intent(
    question: str,
    default_date_col: str = "REQUEST_DATE",
    select_all_default: bool = True,
) -> NLIntent:
    q = question or ""
    intent = NLIntent()

    window = resolve_window(q)
    if window:
        intent.has_time_window = True
        intent.explicit_dates = TimeWindow(start=window.start, end=window.end)
        intent.date_column = default_date_col
    else:
        intent.has_time_window = None
        intent.date_column = default_date_col

    if _RE_COUNT.search(q):
        intent.agg = "count"
    elif _RE_AVG.search(q):
        intent.agg = "avg"
    elif _RE_SUM.search(q):
        intent.agg = "sum"
    elif _RE_MIN.search(q):
        intent.agg = "min"
    elif _RE_MAX.search(q):
        intent.agg = "max"

    m = _RE_BY.search(q)
    if m:
        dim = (m.group(1) or m.group(2) or "").strip().lower()
        if dim in DIM_SYNONYMS:
            intent.group_by = DIM_SYNONYMS[dim]
        else:
            for key, value in DIM_SYNONYMS.items():
                if key in dim:
                    intent.group_by = value
                    break
    if intent.group_by is None:
        lowered = q.lower()
        for key, value in DIM_SYNONYMS.items():
            if key in lowered:
                intent.group_by = value
                break

    top = _RE_TOP.search(q)
    bottom = _RE_BOTTOM.search(q)
    if top:
        n = _to_int(top.group(2)) or 10
        intent.top_n = n
        intent.user_requested_top_n = True
        intent.sort_desc = True
    elif bottom:
        n = _to_int(bottom.group(2)) or 10
        intent.top_n = n
        intent.user_requested_top_n = True
        intent.sort_desc = False

    if _RE_GROSS.search(q):
        intent.measure_sql = (
            "NVL(CONTRACT_VALUE_NET_OF_VAT,0) + "
            "CASE WHEN NVL(VAT,0) BETWEEN 0 AND 1 "
            "THEN NVL(CONTRACT_VALUE_NET_OF_VAT,0) * NVL(VAT,0) "
            "ELSE NVL(VAT,0) END"
        )
    elif _RE_NET.search(q) or intent.agg in {"sum", "avg", "min", "max"}:
        intent.measure_sql = "NVL(CONTRACT_VALUE_NET_OF_VAT,0)"

    intent.wants_all_columns = (
        select_all_default if intent.group_by is None and intent.agg is None else False
    )

    if intent.top_n and not intent.sort_by:
        intent.sort_by = intent.measure_sql or "NVL(CONTRACT_VALUE_NET_OF_VAT,0)"

    intent.notes["q"] = q
    return intent
