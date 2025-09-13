from __future__ import annotations

import re
from datetime import date, timedelta
from typing import Any, Dict, List, Optional


# --- Patterns ---------------------------------------------------------------

DATE_BETWEEN = re.compile(
    r"\bbetween\s*(\d{4}-\d{2}-\d{2})\s*(?:and|to|-)\s*(\d{4}-\d{2}-\d{2})\b",
    re.I,
)
DATE_TWO = re.compile(r"\b(\d{4}-\d{2}-\d{2})\b.*?\b(\d{4}-\d{2}-\d{2})\b", re.I)
LAST_MONTH = re.compile(r"\blast\s+month\b", re.I)
THIS_MONTH = re.compile(r"\bthis\s+month\b", re.I)

DIM_FILTER = re.compile(r"\bdimension\s*([1-4])\s*=\s*([^\s,;]+)", re.I)
ITEM_FILTER = re.compile(r"\bitem\s*=\s*([^\s,;]+)", re.I)

# broad metric cues (don’t force “sum”, catch “total”, “sales”, etc.)
METRIC_CUES = re.compile(r"\b(total|sum|sales|revenue|amount|value|net|gross)\b", re.I)

# table cues (just to lower clarify pressure; real joins are model-driven)
TABLE_CUES = re.compile(
    r"\b(debtor[_\s]?trans(?:_details)?|debtors[_\s]?master|supp[_\s]?trans|gl[_\s]?trans|bank[_\s]?trans|stock[_\s]?moves|item(?:s|_master)?)\b",
    re.I,
)


def _month_bounds(d: date) -> tuple[date, date]:
    start = d.replace(day=1)
    if start.month == 12:
        next_start = start.replace(year=start.year + 1, month=1, day=1)
    else:
        next_start = start.replace(month=start.month + 1, day=1)
    end = next_start - timedelta(days=1)
    return start, end


def detect_date_range(text: str) -> Optional[Dict[str, Any]]:
    t = (text or "").strip().lower()

    m = DATE_BETWEEN.search(t)
    if m:
        return {"start": m.group(1), "end": m.group(2), "grain": "day"}

    m = DATE_TWO.search(t)
    if m:
        return {"start": m.group(1), "end": m.group(2), "grain": "day"}

    if LAST_MONTH.search(t):
        today = date.today()
        first_this, _ = _month_bounds(today)
        last_month_end = first_this - timedelta(days=1)
        last_month_start, _ = _month_bounds(last_month_end)
        return {
            "start": last_month_start.isoformat(),
            "end": last_month_end.isoformat(),
            "grain": "month",
        }

    if THIS_MONTH.search(t):
        today = date.today()
        start, end = _month_bounds(today)
        return {"start": start.isoformat(), "end": end.isoformat(), "grain": "month"}

    return None


def detect_metric(text: str) -> Optional[str]:
    t = (text or "")
    if METRIC_CUES.search(t):
        return "sum_sales"
    return None


def detect_table_cues(text: str) -> List[str]:
    return [m.group(1).lower().replace(" ", "_") for m in TABLE_CUES.finditer(text or "")]


def extract_simple_filters(text: str) -> List[Dict[str, str]]:
    filters: List[Dict[str, str]] = []

    for m in DIM_FILTER.finditer(text or ""):
        dim_no, val = m.group(1), m.group(2)
        filters.append({"type": "dimension", "key": f"dimension{dim_no}", "value": val})

    m = ITEM_FILTER.search(text or "")
    if m:
        filters.append({"type": "item", "key": "item_code", "value": m.group(1)})

    return filters


def first_questions_for(question: str, hints: Dict[str, Any]) -> List[str]:
    qs: List[str] = []
    combo = f"{question} :: {hints}".lower()

    if not hints.get("date_range"):
        qs.append("What date range should we use (e.g., last month, between 2025-08-01 and 2025-08-31)?")

    if not hints.get("metric_hint"):
        qs.append("Which metric should we compute (e.g., sum of net sales, count of invoices)?")

    if not hints.get("table_cues"):
        qs.append("Which tables should we use (e.g., debtor_trans, debtors_master, gl_trans)?")

    return qs


def make_fa_hints(mem_engine, prefixes: List[str], question: str) -> Dict[str, Any]:
    hints: Dict[str, Any] = {
        "prefixes": prefixes or [],
        "keywords": [],
    }

    dr = detect_date_range(question)
    if dr:
        hints["date_range"] = dr

    metric = detect_metric(question)
    if metric:
        hints["metric_hint"] = metric

    tables = detect_table_cues(question)
    if tables:
        hints["table_cues"] = tables

    filters = extract_simple_filters(question)
    if filters:
        hints["filters"] = filters

    hints["keywords"] = [w for w in re.split(r"[^a-z0-9_]+", question.lower()) if w][:32]

    hints["questions"] = first_questions_for(question, hints)
    return hints


def parse_admin_answer(answer: str) -> Dict[str, Any]:
    """Placeholder for backward compatibility; returns empty overrides."""
    return {}


