from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta
from calendar import monthrange
import re
from typing import Any, Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Deterministic NL intent parsing used by the /dw/answer deterministic flow.
# ---------------------------------------------------------------------------

DIM_MAP: Dict[str, str] = {
    "owner department": "OWNER_DEPARTMENT",
    "owner departments": "OWNER_DEPARTMENT",
    "department": "OWNER_DEPARTMENT",
    "departments": "OWNER_DEPARTMENT",
    "department oul": "DEPARTMENT_OUL",
    "department_oul": "DEPARTMENT_OUL",
    "manager": "DEPARTMENT_OUL",
    "contract owner": "CONTRACT_OWNER",
    "owner": "CONTRACT_OWNER",
    "stakeholder": "CONTRACT_STAKEHOLDER_1",
    "stakeholders": "CONTRACT_STAKEHOLDER_1",
    "status": "CONTRACT_STATUS",
    "entity": "ENTITY",
    "entity no": "ENTITY_NO",
    "entity number": "ENTITY_NO",
    "entity #": "ENTITY_NO",
}

GROSS_SQL = (
    "NVL(CONTRACT_VALUE_NET_OF_VAT,0) + "
    "CASE WHEN NVL(VAT,0) BETWEEN 0 AND 1 "
    "THEN NVL(CONTRACT_VALUE_NET_OF_VAT,0) * NVL(VAT,0) "
    "ELSE NVL(VAT,0) END"
)

NET_SQL = "NVL(CONTRACT_VALUE_NET_OF_VAT,0)"

# Map common synonyms -> canonical column names
_PROJECTION_MAP: Dict[str, str] = {
    r"\bcontract\s*id\b": "CONTRACT_ID",
    r"\bowner\b": "CONTRACT_OWNER",
    r"\bowner\s*department\b": "OWNER_DEPARTMENT",
    r"\bdepartment\s*oul\b": "DEPARTMENT_OUL",
    r"\bentity\b": "ENTITY",
    r"\bentity\s*no\b": "ENTITY_NO",
    r"\brequest\s*date\b": "REQUEST_DATE",
    r"\bstart\s*date\b": "START_DATE",
    r"\bend\s*date\b": "END_DATE",
    r"\bstatus\b": "CONTRACT_STATUS",
}


def _detect_projection_list(q: str) -> Optional[List[str]]:
    """Return canonical column list if the user enumerates columns."""

    m = re.search(r"\(([^)]+)\)", q)
    if not m:
        return None
    raw = m.group(1)
    parts = [
        p.strip().lower()
        for p in re.split(r"[;,/]|\band\b", raw, flags=re.I)
        if p.strip()
    ]
    cols: List[str] = []
    for token in parts:
        for pat, col in _PROJECTION_MAP.items():
            if re.search(pat, token, re.I):
                cols.append(col)
                break
    return cols or None


def _pick_date_column(q: str, *, prefer_overlap_default: bool = True) -> str:
    """REQUEST_DATE only for explicit 'requested'; 'expiring' -> END_DATE; else OVERLAP."""

    if re.search(r"\bexpir(?:e|ing|y)\b", q, re.I):
        return "END_DATE"
    if re.search(r"\brequest(ed)?\b", q, re.I) or re.search(r"\bREQUEST_DATE\b", q):
        return "REQUEST_DATE"
    return "OVERLAP" if prefer_overlap_default else "REQUEST_DATE"

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
}


def _word_to_int(token: str) -> Optional[int]:
    token = token.strip().lower()
    if not token:
        return None
    if token.isdigit():
        try:
            return int(token)
        except Exception:
            return None
    return _NUM_WORDS.get(token)


TOP_RE = re.compile(r"\btop\s+(\d+|[a-z]+)\b", re.I)
LAST_N_DAYS_RE = re.compile(r"\blast\s+(\d+|[a-z]+)\s+days?\b", re.I)
LAST_N_WEEKS_RE = re.compile(r"\blast\s+(\d+|[a-z]+)\s+weeks?\b", re.I)
LAST_WEEK_RE = re.compile(r"\blast\s+week\b", re.I)
LAST_N_MONTHS_RE = re.compile(r"\blast\s+(\d+|[a-z]+)\s+months?\b", re.I)
LAST_12_MONTHS_RE = re.compile(r"\blast\s+(?:12|twelve)\s+months?\b", re.I)
LAST_MONTH_RE = re.compile(r"\blast\s+month\b", re.I)
LAST_N_QUARTERS_RE = re.compile(r"\blast\s+(\d+|[a-z]+)\s+quarters?\b", re.I)
NEXT_N_DAYS_RE = re.compile(r"\bnext\s+(\d+|[a-z]+)\s+days?\b", re.I)
EXPIRING_IN_RE = re.compile(r"\bexpir(?:e|es|ing)\s+in\s+(\d+|[a-z]+)\s+days?\b", re.I)
BETWEEN_RE = re.compile(
    r"\bbetween\s+(\d{4}-\d{2}-\d{2})\s+and\s+(\d{4}-\d{2}-\d{2})",
    re.I,
)
LAST_QUARTER_RE = re.compile(r"\blast\s+quarter\b", re.I)
YEAR_YTD_RE = re.compile(r"\b(20\d{2})\s*ytd\b", re.I)
IN_YEAR_RE = re.compile(r"\bin\s*(20\d{2})\b", re.I)


def _add_months(dt: date, months: int) -> date:
    month_index = dt.month - 1 + months
    year = dt.year + month_index // 12
    month = month_index % 12 + 1
    day = min(dt.day, monthrange(year, month)[1])
    return date(year, month, day)


def _month_bounds(dt: date) -> tuple[date, date]:
    first = dt.replace(day=1)
    next_first = _add_months(first, 1)
    last = next_first - timedelta(days=1)
    return first, last


def _last_month_bounds(today: Optional[date] = None) -> tuple[date, date]:
    today = today or date.today()
    first_this, _ = _month_bounds(today)
    last_prev = first_this - timedelta(days=1)
    first_prev = last_prev.replace(day=1)
    return first_prev, last_prev


def _last_n_months_bounds(n: int, today: Optional[date] = None) -> tuple[date, date]:
    today = today or date.today()
    end = today
    start_month = _add_months(today.replace(day=1), -n)
    return start_month, end


def _last_quarter_bounds(today: Optional[date] = None) -> tuple[date, date]:
    today = today or date.today()
    q = (today.month - 1) // 3 + 1
    prev_q = q - 1 if q > 1 else 4
    year = today.year if q > 1 else today.year - 1
    start_month = 3 * (prev_q - 1) + 1
    start = date(year, start_month, 1)
    _, end = _month_bounds(_add_months(start, 2))
    return start, end


def _last_n_quarters_bounds(n: int, today: Optional[date] = None) -> tuple[date, date]:
    today = today or date.today()
    current_q = (today.month - 1) // 3 + 1
    start_month = 3 * (current_q - 1) + 1
    first_this_q = date(today.year, start_month, 1)
    end = today
    start = _add_months(first_this_q, -(3 * n))
    return start, end


def _last_week_bounds(today: Optional[date] = None) -> tuple[date, date]:
    today = today or date.today()
    end = today
    start = today - timedelta(days=7)
    return start, end


def _last_n_days_bounds(n: int, today: Optional[date] = None) -> tuple[date, date]:
    today = today or date.today()
    end = today
    start = today - timedelta(days=n)
    return start, end


@dataclass
class NLIntent:
    # core
    has_time_window: Optional[bool] = None
    date_column: Optional[str] = None
    explicit_dates: Optional[Dict[str, str]] = None
    expire: Optional[bool] = None
    # aggregation / grouping
    agg: Optional[str] = None
    group_by: Optional[str] = None
    measure_sql: Optional[str] = None
    sort_by: Optional[str] = None
    sort_desc: Optional[bool] = True
    top_n: Optional[int] = None
    user_requested_top_n: Optional[bool] = None
    # projection / search
    wants_all_columns: Optional[bool] = True
    projection: Optional[List[str]] = None
    full_text_search: Optional[bool] = False
    fts_tokens: Optional[List[str]] = None
    # notes / extras
    notes: Dict[str, Any] = field(default_factory=dict)


def parse_intent(
    question: str,
    *,
    prefer_overlap_default: bool = True,
    require_window_for_expire: bool = True,
    full_text_search: bool = False,
) -> NLIntent:
    q = (question or "").strip()
    ql = q.lower()
    today = date.today()
    intent = NLIntent(notes={"q": q}, full_text_search=full_text_search)

    if proj := _detect_projection_list(q):
        intent.projection = proj
        intent.wants_all_columns = False

    # --- Top N ------------------------------------------------------------
    if m := TOP_RE.search(ql):
        raw_top = m.group(1)
        n_val = _word_to_int(raw_top)
        if n_val is not None:
            intent.top_n = n_val
            intent.user_requested_top_n = True
            intent.sort_desc = True

    # --- Aggregations & measure ------------------------------------------
    intent.measure_sql = intent.measure_sql or NET_SQL
    if "gross" in ql:
        intent.measure_sql = GROSS_SQL
        intent.agg = "sum"
        intent.sort_by = GROSS_SQL
    elif "contract value" in ql or "net" in ql:
        intent.measure_sql = NET_SQL
        intent.agg = "sum" if (" by " in ql or " per " in ql) else intent.agg
        intent.sort_by = NET_SQL
    if "count" in ql or "(count)" in ql:
        intent.agg = "count"
        intent.wants_all_columns = False

    # --- Group by / per dimension ----------------------------------------
    if " by " in ql or " per " in ql:
        match = re.search(r"\b(?:by|per)\s+([a-z0-9_ \-]+)", q, re.I)
        if match:
            raw_dim = match.group(1)
            raw_dim = re.split(r"\b(last|next|this)\b", raw_dim, maxsplit=1)[0]
            raw_dim = raw_dim.split(" for ", 1)[0]
            raw_dim = raw_dim.split(" in ", 1)[0]
            raw_dim = raw_dim.split(" of ", 1)[0]
            raw_dim = raw_dim.strip().lower()
            raw_dim = raw_dim.rstrip(",")
            gb = DIM_MAP.get(raw_dim)
            if gb:
                intent.group_by = gb
                intent.wants_all_columns = False
                if intent.agg is None:
                    intent.agg = "sum"
                    intent.sort_by = intent.sort_by or intent.measure_sql

    # --- Window detection -------------------------------------------------
    if m := BETWEEN_RE.search(ql):
        start_s, end_s = m.group(1), m.group(2)
        intent.has_time_window = True
        intent.explicit_dates = {"start": start_s, "end": end_s}

    def _set_dates(bounds: Tuple[date, date]) -> None:
        start, end = bounds
        intent.has_time_window = True
        intent.explicit_dates = {"start": start.isoformat(), "end": end.isoformat()}

    if LAST_MONTH_RE.search(ql):
        s, e = _last_month_bounds(today)
        _set_dates((s, e))
    elif m := LAST_N_MONTHS_RE.search(ql):
        n_raw = m.group(1)
        n_val = _word_to_int(n_raw)
        if n_val:
            s, e = _last_n_months_bounds(n_val, today)
            _set_dates((s, e))
    elif LAST_QUARTER_RE.search(ql):
        s, e = _last_quarter_bounds(today)
        _set_dates((s, e))
    elif m := LAST_N_QUARTERS_RE.search(ql):
        n_raw = m.group(1)
        n_val = _word_to_int(n_raw)
        if n_val:
            s, e = _last_n_quarters_bounds(n_val, today)
            _set_dates((s, e))
    elif LAST_WEEK_RE.search(ql):
        s, e = _last_week_bounds(today)
        _set_dates((s, e))
    elif m := LAST_N_WEEKS_RE.search(ql):
        n_raw = m.group(1)
        n_val = _word_to_int(n_raw)
        if n_val:
            s, e = _last_n_days_bounds(n_val * 7, today)
            _set_dates((s, e))
    elif m := LAST_N_DAYS_RE.search(ql):
        n_raw = m.group(1)
        n_val = _word_to_int(n_raw)
        if n_val:
            s, e = _last_n_days_bounds(n_val, today)
            _set_dates((s, e))
    elif m := NEXT_N_DAYS_RE.search(ql):
        n_raw = m.group(1)
        n_val = _word_to_int(n_raw)
        if n_val:
            s = today
            e = today + timedelta(days=n_val)
            _set_dates((s, e))
    elif m := EXPIRING_IN_RE.search(ql):
        n_raw = m.group(1)
        n_val = _word_to_int(n_raw)
        if n_val:
            s = today
            e = today + timedelta(days=n_val)
            _set_dates((s, e))
        intent.expire = True
    elif m := YEAR_YTD_RE.search(ql):
        year = int(m.group(1))
        start = date(year, 1, 1)
        end = today
        _set_dates((start, end))
    elif m := IN_YEAR_RE.search(ql):
        year = int(m.group(1))
        start = date(year, 1, 1)
        end = date(year, 12, 31)
        _set_dates((start, end))

    # --- Date column selection -------------------------------------------
    intent.date_column = _pick_date_column(q, prefer_overlap_default=prefer_overlap_default)

    if intent.has_time_window and intent.explicit_dates and LAST_12_MONTHS_RE.search(ql):
        intent.date_column = "REQUEST_DATE"

    if intent.expire:
        intent.date_column = "END_DATE"
        if require_window_for_expire and not intent.explicit_dates:
            s = today
            e = today + timedelta(days=30)
            _set_dates((s, e))

    # projection defaults --------------------------------------------------
    if intent.agg or intent.group_by:
        intent.wants_all_columns = False
    elif intent.wants_all_columns is None:
        intent.wants_all_columns = True

    # basic FTS tokens if requested ---------------------------------------
    if full_text_search:
        words = re.findall(r"[A-Za-z0-9_]{3,}", q)
        tokens = [w.upper() for w in words if w]
        intent.fts_tokens = tokens or None

    return intent


# ---------------------------------------------------------------------------
# Legacy helpers retained for compatibility with existing modules/tests.
# ---------------------------------------------------------------------------

DIM_SYNONYMS = {
    "owner department": "OWNER_DEPARTMENT",
    "department": "OWNER_DEPARTMENT",
    "manager": "DEPARTMENT_OUL",
    "oul": "DEPARTMENT_OUL",
    "contract owner": "CONTRACT_OWNER",
    "owner": "CONTRACT_OWNER",
    "entity": "ENTITY",
    "entity no": "ENTITY_NO",
    "stakeholder": "CONTRACT_STAKEHOLDER_1",
    "stakeholders": "CONTRACT_STAKEHOLDER_1",
}

BY_RE = re.compile(r"\b(?:by|per)\s+([a-z0-9_ \-]+)\b", re.I)
REQUESTED_RE = re.compile(r"\b(request|requested|request\s+date|طلب)\b", re.I)
EXPIRE_RE = re.compile(r"\b(expire|expiry|expiring|ينتهي)\b", re.I)
COUNT_RE = re.compile(r"\bcount\b|\(count\)", re.I)
GROSS_RE = re.compile(r"\bgross\b", re.I)
NET_RE = re.compile(r"\bnet\b|\bcontract\s*value\b", re.I)
TOP_RE_V1 = re.compile(r"\btop\s+(\d+)\b", re.I)


@dataclass
class DWIntent:
    agg: str | None = None
    group_by: str | None = None
    sort_by: str | None = None
    sort_desc: bool | None = None
    top_n: int | None = None
    date_column: str | None = None
    has_time_window: bool | None = None
    explicit_dates: dict | None = None
    wants_all_columns: bool | None = None
    user_requested_top_n: bool | None = None
    full_text_search: bool | None = None
    expire: bool | None = None
    measure_sql: str | None = None
    fts_tokens: List[str] = field(default_factory=list)


def parse_intent_v1(
    question: str,
    *,
    default_measure: str = NET_SQL,
    select_all_default: bool = True,
    accuracy_first: bool = True,
) -> DWIntent:
    q = (question or "").strip()
    ql = q.lower()
    intent = DWIntent()
    intent.measure_sql = default_measure
    intent.wants_all_columns = bool(select_all_default)

    if COUNT_RE.search(q):
        intent.agg = "count"
        intent.wants_all_columns = False

    if GROSS_RE.search(q):
        intent.measure_sql = GROSS_SQL
    elif NET_RE.search(q):
        intent.measure_sql = default_measure

    if "top" in ql or "highest" in ql:
        intent.sort_by = intent.measure_sql
        intent.sort_desc = True
        intent.user_requested_top_n = True
        m = TOP_RE_V1.search(q)
        if m:
            try:
                intent.top_n = int(m.group(1))
            except Exception:
                pass

    match = BY_RE.search(q)
    if match:
        group_raw = match.group(1)
        group_raw = re.split(r"\b(last|next|this)\b", group_raw, maxsplit=1)[0]
        group_raw = group_raw.strip(" ,")
        mapped = DIM_SYNONYMS.get(group_raw.lower())
        if mapped:
            intent.group_by = mapped

    intent.date_column = "OVERLAP" if accuracy_first else "REQUEST_DATE"
    if REQUESTED_RE.search(ql):
        intent.date_column = "REQUEST_DATE"
    if EXPIRE_RE.search(ql):
        intent.expire = True

    if intent.group_by or intent.agg:
        intent.wants_all_columns = False

    return intent


# Legacy NL intent helpers (used in tests/attempt flows)
_TOP = re.compile(r"\btop\s*(\d+)\b", re.I)
_COUNT = re.compile(r"\bcount\b|\(count\)", re.I)
_GROSS = re.compile(r"\bgross\b", re.I)
_NET = re.compile(r"\bnet\b|\bcontract value\b", re.I)
_BY = re.compile(r"\bby\s+([a-zA-Z_ ]+)|\bper\s+([a-zA-Z_ ]+)", re.I)
_EXPIRE = re.compile(r"\bexpir\w*\b", re.I)
_LAST_N_MONTHS = re.compile(r"\blast\s+(\d+)\s+months?\b", re.I)
_LAST_MONTH = re.compile(r"\blast\s+month\b", re.I)
_LAST_N_DAYS = re.compile(r"\blast\s+(\d+)\s+days?\b", re.I)
_THREE_MONTHS = re.compile(r"\blast\s+3\s+months?\b", re.I)
_BY_STATUS = re.compile(r"\bby\s+status\b", re.I)
_LIST_COLS = re.compile(r"\(([^)]+)\)\s*$")

_REQ_CUES = re.compile(r"\b(request(ed)?|requested|request date|submitted)\b", re.I)
_START_CUES = re.compile(r"\b(start(s|ed|ing)?|begin(s|ning)?)\b", re.I)
_END_CUES = re.compile(r"\b(end(s|ed|ing)?|expire(s|d|ing)?|expiry|expiring)\b", re.I)
_CONTRACTS = re.compile(r"\bcontract(s)?\b", re.I)
_STAKE = re.compile(r"\bstakeholder(s)?\b", re.I)
_DEPT = re.compile(r"\b(owner\s*department|department)\b", re.I)
_OUL = re.compile(r"\b(oul|manager|department_oul)\b", re.I)
_ENTITY = re.compile(r"\b(entity|entity\s*no)\b", re.I)


from .utils import (  # noqa: E402
    last_month,
    last_n_days,
    last_n_months,
    mentions_requested,
    today_utc,
)


def normalize_dimension(dim: str) -> Optional[str]:
    d = (dim or "").strip().lower()
    if not d:
        return None
    if "stakeholder" in d:
        return "CONTRACT_STAKEHOLDER_1"
    if "owner department" in d or d == "department":
        return "OWNER_DEPARTMENT"
    if "entity" in d:
        return "ENTITY_NO"
    if "owner" in d:
        return "CONTRACT_OWNER"
    if "status" in d:
        return "CONTRACT_STATUS"
    if "oul" in d or "manager" in d:
        return "DEPARTMENT_OUL"
    return None


def _set_window(intent: NLIntent, start: str, end: str) -> None:
    intent.has_time_window = True
    intent.explicit_dates = {"start": start, "end": end}


def parse_intent_legacy(q: str) -> NLIntent:
    q = (q or "").strip()
    now = today_utc()
    intent = NLIntent(notes={"q": q})

    m = _TOP.search(q)
    if m:
        intent.top_n = int(m.group(1))
        intent.user_requested_top_n = True

    if _COUNT.search(q):
        intent.agg = "count"

    if _GROSS.search(q):
        intent.measure_sql = GROSS_SQL
    else:
        intent.measure_sql = NET_SQL

    if _STAKE.search(q):
        intent.group_by = "CONTRACT_STAKEHOLDER_1"
    elif _DEPT.search(q):
        intent.group_by = "OWNER_DEPARTMENT"
    elif _OUL.search(q):
        intent.group_by = "DEPARTMENT_OUL"
    elif _ENTITY.search(q):
        intent.group_by = "ENTITY_NO"
    else:
        m = _BY.search(q)
        if m:
            dim = (m.group(1) or m.group(2) or "").strip()
            gb = normalize_dimension(dim)
            if gb:
                intent.group_by = gb
        elif _BY_STATUS.search(q):
            intent.group_by = "CONTRACT_STATUS"
            if intent.agg is None:
                intent.agg = "count"

    intent.sort_by = intent.measure_sql
    intent.sort_desc = True

    if _EXPIRE.search(q):
        intent.expire = True

    if _LAST_MONTH.search(q):
        start, end = last_month(now)
        _set_window(intent, start, end)
    elif _THREE_MONTHS.search(q):
        start, end = last_n_months(3, now)
        _set_window(intent, start, end)
    else:
        m = _LAST_N_MONTHS.search(q)
        if m:
            n = int(m.group(1))
            start, end = last_n_months(n, now)
            _set_window(intent, start, end)
        m = _LAST_N_DAYS.search(q)
        if m:
            n = int(m.group(1))
            start, end = last_n_days(n, now)
            _set_window(intent, start, end)

    if intent.expire and not intent.explicit_dates:
        start, end = last_n_days(30, now)
        _set_window(intent, start, end)

    if mentions_requested(q) or _REQ_CUES.search(q):
        intent.date_column = "REQUEST_DATE"
    elif _START_CUES.search(q):
        intent.date_column = "START_DATE"
    elif intent.expire or _END_CUES.search(q):
        intent.date_column = "END_DATE"
    else:
        intent.date_column = "OVERLAP"

    if intent.expire:
        intent.has_time_window = True

    if intent.wants_all_columns is None:
        intent.wants_all_columns = intent.group_by is None

    m = _LIST_COLS.search(q)
    if m:
        cols = [c.strip().upper().replace(" ", "_") for c in m.group(1).split(",") if c.strip()]
        if cols:
            intent.notes["projection"] = cols
            intent.wants_all_columns = False

    return intent


# Backwards-compatible alias for callers that previously imported parse_intent
# expecting the DWIntent-based parser.
parse_intent_dw = parse_intent_v1
