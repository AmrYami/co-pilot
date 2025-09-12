# apps/fa/hints.py
"""FA-specific hint helpers."""

from __future__ import annotations
from datetime import date, timedelta
from typing import Any, Dict, List, Optional


# Questions to ask when specific fields are missing from the structured spec
MISSING_FIELD_QUESTIONS = {
    "date_range": "What date range should we use (e.g., last month, between 2025-08-01 and 2025-08-31)?",
    "tables": "Which tables should we use (e.g., debtor_trans, debtors_master, gl_trans)?",
    "metric": "Which metric should we compute (e.g., sum of net sales, count of invoices)?",
    "entity": "Top by what entity (customer, supplier, item, account, or a dimension)?",
}

# Lightweight domain hints handed to the clarifier
DOMAIN_HINTS = {
    "entities": ["customer", "supplier", "item", "account", "dimension"],
    "table_aliases": [
        "debtor_trans",
        "debtors_master",
        "supp_trans",
        "gl_trans",
        "bank_trans",
        "stock_moves",
        "item_codes",
    ],
    "metric_registry": {"net_sales": "sum(quantity * price * (1-discount))"},
}


def _last_month_bounds() -> tuple[str, str]:
    today = date.today()
    first_this = today.replace(day=1)
    last_day_prev = first_this - timedelta(days=1)
    first_day_prev = last_day_prev.replace(day=1)
    return first_day_prev.isoformat(), last_day_prev.isoformat()


def parse_admin_answer(answer: str) -> Dict[str, Any]:
    """
    Minimal heuristics:
      - if mentions 'invoice' or 'tran_date' -> prefer debtor_trans.tran_date
      - 'last month' -> concrete YYYY-MM-DD range
    Returns a dict that make_fa_hints can merge into its output.
    """
    a = (answer or "").lower()
    out: Dict[str, Any] = {}

    if "tran_date" in a or "invoice" in a:
        out["date_column"] = "debtor_trans.tran_date"

    if "last month" in a:
        start, end = _last_month_bounds()
        out["date_filter"] = {
            "column": out.get("date_column", "tran_date"),
            "op": "between",
            "start": start,
            "end": end,
        }
        out["time_grain"] = "month"

    if "top 10" in a:
        out["limit"] = 10

    return out


def _build(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Build FA-specific hints from a normalized payload."""
    from core.hints import make_hints as core_make_hints
    from apps.fa.adapters import expand_keywords

    q = (payload.get("question") or "").strip()
    prefixes = list(payload.get("prefixes") or [])
    clarifications: Optional[Dict[str, Any]] = payload.get("clarifications") or None
    admin_overrides: Optional[Dict[str, Any]] = payload.get("admin_overrides") or None

    # App-agnostic, lightweight hints (date range, simple eq filters)
    base = core_make_hints(q)

    # FA-specific keyword expansion (customers, invoices, etc.)
    base["keywords"] = expand_keywords(q.split())

    # Apply clarifications when provided (date range, date column, etc.)
    if clarifications:
        if dr := clarifications.get("date_range"):
            # support either dict(start/end) or string alias
            if isinstance(dr, dict):
                base["date_range"] = dr
            elif isinstance(dr, str):
                from core.hints import make_hints as _mh
                dr_parsed = _mh(dr).get("date_range")
                if dr_parsed:
                    base["date_range"] = dr_parsed
        if dc := clarifications.get("date_column"):
            base["date_column"] = dc

    # Always pass-through prefixes to downstream planner
    base["prefixes"] = prefixes

    # Apply admin overrides last
    if admin_overrides:
        base.update(admin_overrides)

    # You can add more FA-specific nudges here later (dimensions, ST codes, etc.)
    return base


def make_fa_hints(*args, **kwargs) -> Dict[str, Any]:
    """Compatible entry point supporting legacy and new call styles."""

    # New-style: single dict positional
    if args and len(args) == 1 and isinstance(args[0], dict):
        return _build(args[0])

    # Legacy: 3 positional args -> (mem_engine, prefixes, question[, clarifications])
    if len(args) >= 3:
        mem_engine, prefixes, question = args[:3]
        clar = args[3] if len(args) > 3 else None
        admin_overrides = args[4] if len(args) > 4 else None
        return _build({
            "mem_engine": mem_engine,
            "prefixes": prefixes,
            "question": question,
            "clarifications": clar,
            "admin_overrides": admin_overrides,
        })

    # Named kwargs (accept either shape)
    if "payload" in kwargs and isinstance(kwargs["payload"], dict):
        return _build(kwargs["payload"])

    return _build({
        "mem_engine": kwargs.get("mem_engine"),
        "prefixes": kwargs.get("prefixes") or [],
        "question": kwargs.get("question") or "",
        "clarifications": kwargs.get("clarifications"),
        "admin_overrides": kwargs.get("admin_overrides"),
    })

