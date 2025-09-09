# apps/fa/hints.py
from __future__ import annotations
from typing import Any, Dict, List, Optional


def _build(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Build FA-specific hints from a normalized payload."""
    from core.hints import make_hints as core_make_hints
    from apps.fa.adapters import expand_keywords

    q = (payload.get("question") or "").strip()
    prefixes = list(payload.get("prefixes") or [])
    clarifications: Optional[Dict[str, Any]] = payload.get("clarifications") or None

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
        return _build({
            "mem_engine": mem_engine,
            "prefixes": prefixes,
            "question": question,
            "clarifications": clar,
        })

    # Named kwargs (accept either shape)
    if "payload" in kwargs and isinstance(kwargs["payload"], dict):
        return _build(kwargs["payload"])

    return _build({
        "mem_engine": kwargs.get("mem_engine"),
        "prefixes": kwargs.get("prefixes") or [],
        "question": kwargs.get("question") or "",
        "clarifications": kwargs.get("clarifications"),
    })

