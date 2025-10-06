# -*- coding: utf-8 -*-
"""Parse /dw/rate free-text comments into structured hints."""
from __future__ import annotations

import re
from typing import Dict, Iterator, List, Optional, Tuple


_DIRECTIVE_RE = re.compile(r"([A-Za-z_][A-Za-z0-9_]*)\s*:")
_FTS_AND_RE = re.compile(r"\s*(?:&|\band\b)\s*", re.IGNORECASE)
_FTS_OR_RE = re.compile(r"\s*(?:\||,|\bor\b)\s*", re.IGNORECASE)
_FLAG_RE = re.compile(r"\(([^)]*)\)\s*$")


def _normalize(value: Optional[str]) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", value).strip()


def _strip_quotes(value: str) -> str:
    text = value.strip()
    if len(text) >= 2 and text[0] in {'"', "'"} and text[-1] == text[0]:
        return text[1:-1]
    return text


def _iter_directives(comment: str) -> Iterator[Tuple[str, str]]:
    """Yield (directive, body) pairs respecting nested ';' inside bodies."""

    if not comment:
        return iter(())

    text = comment.strip()
    if not text:
        return iter(())

    matches = list(_DIRECTIVE_RE.finditer(text))
    if not matches:
        return iter(())

    parts: List[Tuple[str, str]] = []
    for index, match in enumerate(matches):
        key = match.group(1)
        start = match.end()
        end = matches[index + 1].start() if index + 1 < len(matches) else len(text)
        body = text[start:end].strip().rstrip("; ")
        parts.append((key, body))
    return iter(parts)


def _parse_fts(body: str) -> Tuple[List[str], str]:
    if not body:
        return [], "OR"
    operator = "OR"
    if _FTS_AND_RE.search(body):
        tokens = [tok for tok in _FTS_AND_RE.split(body) if tok.strip()]
        operator = "AND"
    else:
        tokens = [tok for tok in _FTS_OR_RE.split(body) if tok.strip()]
    cleaned = []
    for token in tokens:
        normalized = _normalize(_strip_quotes(token))
        if normalized:
            cleaned.append(normalized)
    return cleaned, operator


def _extract_flags(flag_blob: str) -> Tuple[bool, bool, bool]:
    ci = False
    trim = False
    valid = True
    if not flag_blob:
        return ci, trim, False
    seen_any = False
    for flag in flag_blob.split(","):
        name = flag.strip().lower()
        if not name:
            continue
        seen_any = True
        if name in {"ci", "case_insensitive"}:
            ci = True
        elif name == "trim":
            trim = True
        else:
            valid = False
    return ci, trim, valid and seen_any


def _parse_eq_clause(clause: str) -> Optional[Dict[str, object]]:
    if not clause or "=" not in clause:
        return None
    flags_ci = False
    flags_trim = False
    match = _FLAG_RE.search(clause)
    if match:
        ci, trim, valid = _extract_flags(match.group(1))
        if valid:
            flags_ci = ci
            flags_trim = trim
            clause = clause[: match.start()].rstrip()

    lhs, rhs = clause.split("=", 1)
    column = _normalize(lhs).upper()
    value = _strip_quotes(_normalize(rhs))
    if not column or not value:
        return None
    return {"col": column, "val": value, "ci": flags_ci, "trim": flags_trim}


def _parse_eq(body: str) -> List[Dict[str, object]]:
    if not body:
        return []
    clauses: List[Dict[str, object]] = []
    for part in re.split(r";", body):
        parsed = _parse_eq_clause(part.strip())
        if parsed:
            clauses.append(parsed)
    return clauses


def _parse_group_by(body: str) -> Optional[str]:
    if not body:
        return None
    cols = [
        _normalize(segment).upper().replace(" ", "_")
        for segment in body.split(",")
        if _normalize(segment)
    ]
    return cols[0] if cols else None


def _parse_order(body: str) -> Tuple[Optional[str], Optional[bool]]:
    if not body:
        return None, None
    match = re.match(r"(.+?)\s+(asc|desc)$", body.strip(), flags=re.IGNORECASE)
    if match:
        column = _normalize(match.group(1)).upper().replace(" ", "_")
        direction = match.group(2).lower() == "desc"
        return column or None, direction
    column = _normalize(body).upper().replace(" ", "_")
    if not column:
        return None, None
    return column, True


def parse_rate_comment(comment: str) -> Dict[str, object]:
    """Return structured hints extracted from a ``/dw/rate`` comment."""

    out: Dict[str, object] = {
        "fts_tokens": [],
        "fts_operator": "OR",
        "eq_filters": [],
        "group_by": None,
        "gross": None,
        "sort_by": None,
        "sort_desc": None,
    }
    if not comment:
        return out

    eq_filters: List[Dict[str, object]] = []

    for key, body in _iter_directives(comment):
        directive = key.strip().lower()
        if directive == "fts":
            tokens, operator = _parse_fts(body)
            if tokens:
                out["fts_tokens"] = tokens
                out["fts_operator"] = operator or "OR"
        elif directive == "eq":
            eq_filters.extend(_parse_eq(body))
        elif directive == "group_by":
            group = _parse_group_by(body)
            if group:
                out["group_by"] = group
        elif directive == "order_by":
            column, desc = _parse_order(body)
            if column:
                out["sort_by"] = column
                out["sort_desc"] = True if desc is None else bool(desc)
        elif directive == "gross":
            if body:
                lowered = body.strip().lower()
                if lowered in {"true", "false"}:
                    out["gross"] = lowered == "true"

    if eq_filters:
        out["eq_filters"] = eq_filters

    return out


__all__ = ["parse_rate_comment"]
