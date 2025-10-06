# -*- coding: utf-8 -*-
"""Utilities for parsing inline equality filters from free text."""
from __future__ import annotations

import re
from typing import Dict, Iterable, List, Tuple

__all__ = ["parse_eq_filters_from_text", "build_eq_where_and_binds"]

# Column synonyms supported for quick mapping from display names.
_EQ_SYNONYMS = {
    "DEPARTMENT": "OWNER_DEPARTMENT",
    "DEPARTMENTS": "OWNER_DEPARTMENT",
    "OWNER_DEPARTMENT": "OWNER_DEPARTMENT",
    "OWNERDEPARTMENT": "OWNER_DEPARTMENT",
    "OWNER-DEPARTMENT": "OWNER_DEPARTMENT",
    "STAKEHOLDER": "STAKEHOLDER*",
    "STACKHOLDER": "STAKEHOLDER*",
}

_EQ_PATTERN = re.compile(
    r"""
    (?P<col>[A-Za-z0-9_ \-]+?)       # column or display label
    \s*(?:=|==|:| is | equals )\s*   # operator variants
    (?P<q>['\"])?                   # optional opening quote
    (?P<val>[^'\"]+?)               # value until quote or whitespace
    (?P=q)?                          # close same quote if opened
    (?=\s|$)                        # boundary or end
    """,
    re.IGNORECASE | re.VERBOSE,
)


def _canon(value: str | None) -> str:
    cleaned = (value or "").strip()
    if not cleaned:
        return ""
    cleaned = cleaned.replace("/", " ")
    return re.sub(r"[\s\-]+", "_", cleaned).upper()


def _normalize_explicit_columns(columns: Iterable[str]) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    for col in columns or []:
        if not col:
            continue
        text = str(col).strip()
        if not text:
            continue
        key = _canon(text)
        if not key:
            continue
        mapping.setdefault(key, text.strip())
    return mapping


def _map_to_real_column(raw: str, explicit: Dict[str, str]) -> str:
    """Return a safe column name using explicit allow-list + synonyms."""

    canonical = _canon(raw)
    if not canonical:
        return ""
    if canonical in explicit:
        return explicit[canonical]
    alias_target = _EQ_SYNONYMS.get(canonical)
    if alias_target:
        alias_key = _canon(alias_target)
        if alias_key in explicit:
            return explicit[alias_key]
        return alias_target
    return canonical if canonical in explicit else ""


def parse_eq_filters_from_text(
    text: str,
    explicit_columns: Iterable[str],
) -> Tuple[str, List[Dict[str, object]]]:
    """Parse inline equality fragments from ``text``.

    Returns a tuple ``(cleaned_text, filters)`` where ``filters`` is a list of
    dictionaries compatible with :func:`build_eq_where_and_binds`.
    """

    explicit_map = _normalize_explicit_columns(explicit_columns)
    if not explicit_map:
        return text, []

    filters: List[Dict[str, object]] = []
    parts: List[str] = []
    last_index = 0
    for match in _EQ_PATTERN.finditer(text or ""):
        col_name = _map_to_real_column(match.group("col"), explicit_map)
        value = (match.group("val") or "").strip()
        if not col_name or not value:
            continue
        filters.append({"col": col_name, "val": value, "ci": True, "trim": True})
        parts.append((text or "")[last_index : match.start()].rstrip())
        last_index = match.end()
    parts.append((text or "")[last_index:])
    cleaned = " ".join(part for part in parts if part.strip())
    return cleaned.strip(), filters


def build_eq_where_and_binds(
    eq_filters: List[Dict[str, object]]
) -> Tuple[str, Dict[str, object]]:
    """Build an AND-combined equality predicate with binds."""

    predicates: List[str] = []
    binds: Dict[str, object] = {}
    for idx, filt in enumerate(eq_filters or []):
        column = str(filt.get("col") or "").strip()
        value = filt.get("val")
        if not column or value is None:
            continue
        ci = bool(filt.get("ci", True))
        trim = bool(filt.get("trim", True))
        bind_name = f"eq_{idx}"
        binds[bind_name] = value
        column_expr = column
        bind_expr = f":{bind_name}"
        if trim:
            column_expr = f"TRIM({column_expr})"
            bind_expr = f"TRIM({bind_expr})"
        if ci:
            column_expr = f"UPPER({column_expr})"
            bind_expr = f"UPPER({bind_expr})"
        predicates.append(f"({column_expr} = {bind_expr})")
    where_sql = " AND ".join(predicates) if predicates else ""
    return where_sql, binds
