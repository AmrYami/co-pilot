"""Date parsing helpers for the /dw/rate route."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Dict, Optional, Tuple

try:  # Optional dependency used elsewhere in the DW stack
    import dateparser  # type: ignore
except Exception:  # pragma: no cover - optional dependency may not exist
    dateparser = None  # type: ignore

try:  # ``dateutil`` is available in most environments but keep a fallback.
    from dateutil.relativedelta import relativedelta  # type: ignore
except Exception:  # pragma: no cover - optional dependency may not exist
    relativedelta = None  # type: ignore

DateMode = str

DATE_RE_RANGE = re.compile(
    r"\b(\d{4}-\d{2}-\d{2})\s*(?:\.\.|to|-)\s*(\d{4}-\d{2}-\d{2})\b",
    re.IGNORECASE,
)
DATE_RE_DMY_RANGE = re.compile(
    r"\b(\d{1,2}/\d{1,2}/\d{4})\s*(?:\.\.|to|-)\s*(\d{1,2}/\d{1,2}/\d{4})\b",
    re.IGNORECASE,
)
RE_NEXT_N = re.compile(r"\bnext\s+(\d+)\s+(day|days|month|months|year|years)\b", re.I)
RE_LAST_N = re.compile(r"\blast\s+(\d+)\s+(day|days|month|months|year|years)\b", re.I)

WINDOW_KEYWORDS = {
    "last month": "LAST_MONTH",
    "this month": "THIS_MONTH",
    "last quarter": "LAST_QUARTER",
    "this quarter": "THIS_QUARTER",
    "quarter to date": "QTD",
    "qtd": "QTD",
    "year to date": "YTD",
    "ytd": "YTD",
    "this year": "THIS_YEAR",
    "last year": "LAST_YEAR",
    "last 3 months": "LAST_3_MONTHS",
    "next 90 days": "NEXT_90_DAYS",
}

RE_REQUESTED = re.compile(r"\brequested\s+(.*)$", re.IGNORECASE)
RE_ACTIVE = re.compile(r"\bactive\s+(.*)$", re.IGNORECASE)
RE_EXPIRING = re.compile(r"\b(expiring|expires|ending|ends)\s+(.*)$", re.IGNORECASE)
RE_STARTING = re.compile(r"\b(starting|starts|started)\s+(.*)$", re.IGNORECASE)


@dataclass
class DateIntent:
    """Structured representation of a detected date window."""

    mode: DateMode
    column: Optional[str]
    start: date
    end: date
    input_text: str
    order_by_override: Optional[str] = None


def _today() -> date:
    return datetime.now().date()


def _parse_date_str(text: str) -> Optional[date]:
    cleaned = text.strip()
    if not cleaned:
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(cleaned, fmt).date()
        except ValueError:
            continue
    if dateparser is not None:
        parsed = dateparser.parse(cleaned)  # pragma: no cover - depends on optional lib
        if parsed:
            return parsed.date()
    return None


def _month_bounds(ref: date, shift: int = 0) -> Tuple[date, date]:
    if relativedelta is not None:
        start = (ref.replace(day=1) + relativedelta(months=shift)).replace(day=1)
        end = (start + relativedelta(months=1)) - timedelta(days=1)
        return start, end
    # Fallback without dateutil: compute manually
    month_index = ref.month - 1 + shift
    year = ref.year + month_index // 12
    month = month_index % 12 + 1
    start = date(year, month, 1)
    if month == 12:
        end = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        end = date(year, month + 1, 1) - timedelta(days=1)
    return start, end


def _quarter_bounds(ref: date, shift_quarters: int = 0) -> Tuple[date, date]:
    quarter = (ref.month - 1) // 3 + shift_quarters
    year = ref.year + quarter // 4
    quarter = quarter % 4
    start_month = quarter * 3 + 1
    start = date(year, start_month, 1)
    end_month = start_month + 2
    if end_month == 12:
        end = date(year, 12, 31)
    else:
        if end_month in (1, 3, 5, 7, 8, 10, 12):
            end_day = 31
        elif end_month == 2:
            if (year % 400 == 0) or (year % 4 == 0 and year % 100 != 0):
                end_day = 29
            else:
                end_day = 28
        else:
            end_day = 30
        end = date(year, end_month, end_day)
    return start, end


def _window_from_text(text: str, now: Optional[date] = None) -> Optional[Tuple[date, date]]:
    now = now or _today()
    normalized = text.strip().lower()
    if not normalized:
        return None

    keyword = WINDOW_KEYWORDS.get(normalized)
    if keyword == "LAST_MONTH":
        return _month_bounds(now, shift=-1)
    if keyword == "THIS_MONTH":
        return _month_bounds(now, shift=0)
    if keyword == "LAST_QUARTER":
        return _quarter_bounds(now, shift_quarters=-1)
    if keyword == "THIS_QUARTER":
        return _quarter_bounds(now, shift_quarters=0)
    if keyword == "QTD":
        start, _ = _quarter_bounds(now, shift_quarters=0)
        return start, now
    if keyword == "YTD":
        return date(now.year, 1, 1), now
    if keyword == "THIS_YEAR":
        return date(now.year, 1, 1), date(now.year, 12, 31)
    if keyword == "LAST_YEAR":
        year = now.year - 1
        return date(year, 1, 1), date(year, 12, 31)
    if keyword == "LAST_3_MONTHS":
        start, _ = _month_bounds(now, shift=-2)
        return start, now
    if keyword == "NEXT_90_DAYS":
        return now, now + timedelta(days=90)

    match = RE_NEXT_N.search(normalized)
    if match:
        amount = int(match.group(1))
        unit = match.group(2)
        start = now
        if unit.startswith("day"):
            end = now + timedelta(days=amount)
        elif unit.startswith("month"):
            if relativedelta is not None:
                end = now + relativedelta(months=amount)
            else:
                end = now + timedelta(days=30 * amount)
        else:
            if relativedelta is not None:
                end = now + relativedelta(years=amount)
            else:
                end = date(now.year + amount, now.month, now.day)
        return start, end

    match = RE_LAST_N.search(normalized)
    if match:
        amount = int(match.group(1))
        unit = match.group(2)
        end = now
        if unit.startswith("day"):
            start = now - timedelta(days=amount)
        elif unit.startswith("month"):
            if relativedelta is not None:
                start = now - relativedelta(months=amount)
            else:
                start = now - timedelta(days=30 * amount)
        else:
            if relativedelta is not None:
                start = now - relativedelta(years=amount)
            else:
                start = date(now.year - amount, now.month, now.day)
        return start, end

    match = DATE_RE_RANGE.search(normalized)
    if match:
        start = _parse_date_str(match.group(1))
        end = _parse_date_str(match.group(2))
        if start and end:
            return start, end

    match = DATE_RE_DMY_RANGE.search(normalized)
    if match:
        start = _parse_date_str(match.group(1))
        end = _parse_date_str(match.group(2))
        if start and end:
            return start, end

    single = _parse_date_str(normalized)
    if single:
        return single, single

    return None


def _date_sql(intent: DateIntent, settings: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
    require_both = bool(int(settings.get("DW_OVERLAP_REQUIRE_BOTH_DATES", 1) or 0))
    strict_overlap = bool(int(settings.get("DW_OVERLAP_STRICT", 1) or 0))

    binds = {
        "date_start": intent.start,
        "date_end": intent.end,
    }

    if intent.mode == "REQUEST":
        return "REQUEST_DATE BETWEEN :date_start AND :date_end", binds
    if intent.mode == "START_ONLY":
        return "START_DATE BETWEEN :date_start AND :date_end", binds
    if intent.mode == "END_ONLY":
        return "END_DATE BETWEEN :date_start AND :date_end", binds

    clause = "START_DATE <= :date_end AND END_DATE >= :date_start"
    if strict_overlap and require_both:
        clause += " AND START_DATE IS NOT NULL AND END_DATE IS NOT NULL"
    return clause, binds


def _split_parts(comment: str) -> Tuple[str, ...]:
    if not comment:
        return tuple()
    return tuple(
        part.strip()
        for part in re.split(r"[;\n]+", comment)
        if part and part.strip()
    )


def _pick_mode_and_column(key: str) -> Tuple[DateMode, Optional[str], Optional[str]]:
    normalized = key.strip().lower()
    if normalized in {"requested", "request", "request_date"}:
        return "REQUEST", "REQUEST_DATE", None
    if normalized in {"expiring", "ending", "end", "end_date"}:
        return "END_ONLY", "END_DATE", "END_DATE ASC"
    if normalized in {"starting", "start", "start_date", "started"}:
        return "START_ONLY", "START_DATE", None
    return "OVERLAP", None, None


def build_date_clause(
    comment: str,
    settings: Optional[Dict[str, Any]] = None,
) -> Tuple[Optional[DateIntent], Optional[str], Dict[str, Any], Dict[str, Any]]:
    """Parse a comment string and return SQL fragments for date windows."""

    comment = comment or ""
    settings = settings or {}
    parts = _split_parts(comment)

    explicit_key: Optional[str] = None
    window_text: Optional[str] = None
    for part in parts:
        if ":" not in part:
            continue
        key, value = part.split(":", 1)
        normalized_key = key.strip().lower()
        if normalized_key in {
            "date",
            "requested",
            "active",
            "expiring",
            "ending",
            "starting",
            "between",
            "request_between",
            "start_between",
            "end_between",
            "date_from",
            "date_to",
            "on",
        }:
            explicit_key = normalized_key
            window_text = value.strip()
            break

    now = _today()
    mode: DateMode = "OVERLAP"
    column: Optional[str] = None
    order_override: Optional[str] = None

    if explicit_key:
        if explicit_key == "on" and window_text:
            tokens = window_text.split(None, 1)
            if tokens:
                column_token = tokens[0].strip().upper()
                if column_token in {"REQUEST_DATE", "START_DATE", "END_DATE"}:
                    column = column_token
                    mode = (
                        "REQUEST"
                        if column == "REQUEST_DATE"
                        else "START_ONLY"
                        if column == "START_DATE"
                        else "END_ONLY"
                    )
                    if column == "END_DATE":
                        order_override = "END_DATE ASC"
                    window_text = tokens[1].strip() if len(tokens) > 1 else "this month"
        elif explicit_key in {"request_between", "start_between", "end_between"} and window_text:
            range_window = _window_from_text(window_text, now)
            if range_window:
                start, end = range_window
                mode, column, order_override = {
                    "request_between": ("REQUEST", "REQUEST_DATE", None),
                    "start_between": ("START_ONLY", "START_DATE", None),
                    "end_between": ("END_ONLY", "END_DATE", "END_DATE ASC"),
                }[explicit_key]
                intent = DateIntent(mode, column, start, end, window_text, order_override)
                sql, binds = _date_sql(intent, settings)
                return intent, sql, binds, {"key": explicit_key, "text": window_text}
        elif explicit_key in {"date_from", "date_to"}:
            start_value: Optional[date] = None
            end_value: Optional[date] = None
            for part in parts:
                if ":" not in part:
                    continue
                key, value = part.split(":", 1)
                normalized_key = key.strip().lower()
                if normalized_key == "date_from":
                    start_value = _parse_date_str(value.strip())
                elif normalized_key == "date_to":
                    end_value = _parse_date_str(value.strip())
            if start_value or end_value:
                start = start_value or now
                end = end_value or now
                intent = DateIntent("OVERLAP", None, start, end, window_text or "", None)
                sql, binds = _date_sql(intent, settings)
                return intent, sql, binds, {
                    "key": "date_from/to",
                    "from": start.isoformat(),
                    "to": end.isoformat(),
                }
        else:
            mode, column, order_override = _pick_mode_and_column(explicit_key)
            if window_text:
                window = _window_from_text(window_text, now)
                if window:
                    start, end = window
                    intent = DateIntent(mode, column, start, end, window_text, order_override)
                    sql, binds = _date_sql(intent, settings)
                    return intent, sql, binds, {"key": explicit_key, "text": window_text}

    for regex, mode, column, order_override in (
        (RE_REQUESTED, "REQUEST", "REQUEST_DATE", None),
        (RE_EXPIRING, "END_ONLY", "END_DATE", "END_DATE ASC"),
        (RE_STARTING, "START_ONLY", "START_DATE", None),
        (RE_ACTIVE, "OVERLAP", None, None),
    ):
        match = regex.search(comment)
        if not match:
            continue
        captured = match.group(len(match.groups())) if regex is RE_EXPIRING else match.group(1)
        if captured is None:
            continue
        window = _window_from_text(captured.strip(), now)
        if window:
            start, end = window
            intent = DateIntent(mode, column, start, end, captured.strip(), order_override)
            sql, binds = _date_sql(intent, settings)
            key_label = {
                RE_REQUESTED: "requested*",
                RE_EXPIRING: "expiring*",
                RE_STARTING: "starting*",
                RE_ACTIVE: "active*",
            }[regex]
            return intent, sql, binds, {"key": key_label, "text": captured.strip()}

    return None, None, {}, {}
