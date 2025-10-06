# -*- coding: utf-8 -*-
"""Utilities for parsing inline equality filters from free text."""
from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List, Tuple

__all__ = [
    "parse_eq_filters_from_text",
    "build_eq_where_and_binds",
    "extract_eq_filters_from_text",
    "normalize_column",
    "parse_rate_comment",
    "strip_eq_from_text",
]

# Lightweight pattern for simple ``COLUMN = VALUE`` detection used by fallback paths.
_EQ_RE = re.compile(
    r"""
    (?P<col>[A-Za-z0-9_ ]+)\s*
    (?:=|==|is|equals)\s*
    ['\"]?(?P<val>[^'\"]+)['\"]?
    """,
    re.IGNORECASE | re.VERBOSE,
)

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


def normalize_column(col: str) -> str:
    """Normalize a human-friendly column name to uppercase with underscores."""

    return re.sub(r"\s+", "_", (col or "").strip()).upper()


def extract_eq_filters_from_text(
    text: str,
    explicit_columns: Iterable[str],
) -> List[Dict[str, Any]]:
    """Extract equality predicates from free text when column is whitelisted."""

    filters: List[Dict[str, Any]] = []
    allowed = {normalize_column(col): str(col).strip().upper() for col in (explicit_columns or []) if col}
    if not allowed:
        return filters

    for match in _EQ_RE.finditer(text or ""):
        col_key = normalize_column(match.group("col") or "")
        if col_key not in allowed:
            continue
        value = (match.group("val") or "").strip()
        if not value:
            continue
        filters.append({"col": allowed[col_key], "val": value, "ci": True, "trim": True})
    return filters


def strip_eq_from_text(text: str, explicit_columns: Iterable[str]) -> str:
    allowed = {normalize_column(col) for col in (explicit_columns or []) if col}

    def _repl(match: re.Match[str]) -> str:
        col_key = normalize_column(match.group("col") or "")
        return " " if col_key in allowed else match.group(0)

    return _EQ_RE.sub(_repl, text or "")


def parse_rate_comment(comment: str) -> Dict[str, Any]:
    """Parse lightweight ``fts``/``eq`` hints from a rate comment string."""

    intent: Dict[str, Any] = {
        "fts_tokens": [],
        "fts_operator": "OR",
        "eq_filters": [],
        "order_by": None,
        "order_desc": None,
        "group_by": None,
        "gross": None,
    }
    if not comment:
        return intent

    fts_match = re.search(r"fts\s*:\s*([^;]+)", comment, flags=re.IGNORECASE)
    if fts_match:
        tokens = [tok.strip() for tok in fts_match.group(1).split("|") if tok.strip()]
        intent["fts_tokens"] = tokens
        intent["fts_operator"] = "OR"

    for eq_match in re.finditer(r"eq\s*:\s*([^;]+)", comment, flags=re.IGNORECASE):
        expr = eq_match.group(1)
        match = _EQ_RE.search(expr or "")
        if not match:
            continue
        col = normalize_column(match.group("col") or "")
        val = (match.group("val") or "").strip()
        if not col or not val:
            continue
        intent["eq_filters"].append({"col": col, "val": val, "ci": True, "trim": True})

    order = re.search(r"order_by\s*:\s*([A-Za-z0-9_ ]+)\s+(asc|desc)", comment, flags=re.IGNORECASE)
    if order:
        intent["order_by"] = normalize_column(order.group(1) or "")
        intent["order_desc"] = (order.group(2) or "").strip().lower() == "desc"

    group = re.search(r"group_by\s*:\s*([A-Za-z0-9_ ]+)", comment, flags=re.IGNORECASE)
    if group:
        intent["group_by"] = normalize_column(group.group(1) or "")

    gross = re.search(r"gross\s*:\s*(true|false)", comment, flags=re.IGNORECASE)
    if gross:
        intent["gross"] = (gross.group(1) or "").strip().lower() == "true"

    return intent
