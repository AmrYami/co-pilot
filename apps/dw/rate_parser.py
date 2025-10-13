from __future__ import annotations

import re
from typing import Any, Dict, List

try:  # pragma: no cover - optional dependency during tests
    from apps.dw.settings import get_setting
except Exception:  # pragma: no cover - fallback when settings helper unavailable
    def get_setting(*_args, **kwargs):  # type: ignore[return-type]
        return kwargs.get("default")

_OR_SPLIT = re.compile(r"\s+or\s+", re.IGNORECASE)

NEG_EQ_PAT = r"(?:!=|<>|does\s*not\s*equal|doesn['’]?t\s*equal|is\s*not)"
NOT_CONTAINS_PAT = r"(?:does\s*not\s*(?:contain|have|include)|not\s*(?:like|contain|have|include)|doesn['’]?t\s*(?:contain|have|include)|without)"
IS_EMPTY_PAT = r"(?:is\s*empty|is\s*null|=\s*''\s*)"
IS_NOT_EMPTY_PAT = r"(?:is\s*not\s*empty|is\s*not\s*null|<>\s*''\s*)"


def _split_directives(comment: str) -> List[str]:
    return [p.strip() for p in (comment or "").split(";") if p.strip()]


def _parse_list(val: str) -> List[str]:
    if " or " in val.lower():
        parts = _OR_SPLIT.split(val)
    else:
        parts = [p.strip() for p in re.split(r",", val) if p.strip()]
    return [p.strip() for p in parts if p.strip()]


def _alias_expand(col: str) -> List[str]:
    aliases = (get_setting("DW_EQ_ALIAS_COLUMNS", scope="namespace") or {}).copy()
    col_up = col.strip().upper()
    if col_up in aliases:
        return [c.strip() for c in aliases[col_up]]
    return [col_up]


def parse_rate_comment(comment: str) -> Dict[str, Any]:
    intent: Dict[str, Any] = {
        "fts_groups": [],
        "eq": {},
        "neq": {},
        "contains": {},
        "not_contains": {},
        "empty": [],
        "empty_any": [],
        "empty_all": [],
        "not_empty": [],
        "order_by": None,
    }
    parts = _split_directives(comment or "")
    for p in parts:
        low = p.lower()
        if low.startswith("fts:"):
            vals = _parse_list(p.split(":", 1)[1])
            for v in vals:
                if v:
                    intent["fts_groups"].append([v])
            continue
        if low.startswith("eq:"):
            expr = p.split(":", 1)[1]
            m = re.match(r"\s*([^=]+?)\s*=\s*(.+)$", expr.strip(), flags=re.IGNORECASE)
            if m:
                col, vals = m.group(1).strip(), _parse_list(m.group(2))
                cols = _alias_expand(col)
                for c in cols:
                    intent["eq"].setdefault(c, [])
                    intent["eq"][c].extend(vals)
            continue
        if low.startswith("neq:"):
            expr = p.split(":", 1)[1]
            m = re.match(
                r"\s*([^=<>]+?)\s*(?:!=|<>|=)\s*(.+)$", expr.strip(), flags=re.IGNORECASE
            )
            if m:
                col, vals = m.group(1).strip(), _parse_list(m.group(2))
                cols = _alias_expand(col)
                for c in cols:
                    intent["neq"].setdefault(c, [])
                    intent["neq"][c].extend(vals)
            continue
        if low.startswith("contains:"):
            expr = p.split(":", 1)[1]
            m = re.match(r"\s*([^=]+?)\s*=\s*(.+)$", expr.strip(), flags=re.IGNORECASE)
            if m:
                col, vals = m.group(1).strip(), _parse_list(m.group(2))
                cols = _alias_expand(col)
                for c in cols:
                    intent["contains"].setdefault(c, [])
                    intent["contains"][c].extend(vals)
            continue
        if low.startswith("not_contains:") or low.startswith("not-like:"):
            expr = p.split(":", 1)[1]
            m = re.match(r"\s*([^=]+?)\s*=\s*(.+)$", expr.strip(), flags=re.IGNORECASE)
            if m:
                col, vals = m.group(1).strip(), _parse_list(m.group(2))
                cols = _alias_expand(col)
                for c in cols:
                    intent["not_contains"].setdefault(c, [])
                    intent["not_contains"][c].extend(vals)
            continue
        if low.startswith("empty_all:"):
            cols = [c.strip() for c in p.split(":", 1)[1].split(",") if c.strip()]
            intent["empty_all"].append([c.upper() for c in cols])
            continue
        if low.startswith("empty_any:"):
            cols = [c.strip() for c in p.split(":", 1)[1].split(",") if c.strip()]
            intent["empty_any"].append([c.upper() for c in cols])
            continue
        if low.startswith("empty:"):
            cols = [c.strip() for c in p.split(":", 1)[1].split(",") if c.strip()]
            for c in cols:
                intent["empty"].append([c.upper()])
            continue
        if low.startswith("not_empty:"):
            cols = [c.strip() for c in p.split(":", 1)[1].split(",") if c.strip()]
            for c in cols:
                intent["not_empty"].append([c.upper()])
            continue
        if low.startswith("order_by:"):
            intent["order_by"] = p.split(":", 1)[1].strip()
            continue

        m = re.search(
            r"^\s*([\w\.\s]+?)\s+" + IS_EMPTY_PAT + r"\s*$", p, flags=re.IGNORECASE
        )
        if m:
            intent["empty"].append([m.group(1).strip().upper()])
            continue
        m = re.search(
            r"^\s*([\w\.\s]+?)\s+" + IS_NOT_EMPTY_PAT + r"\s*$", p, flags=re.IGNORECASE
        )
        if m:
            intent["not_empty"].append([m.group(1).strip().upper()])
            continue

        m = re.search(
            r"^\s*([\w\.\s]+?)\s+" + NEG_EQ_PAT + r"\s+(.+)$", p, flags=re.IGNORECASE
        )
        if m:
            col, vals = m.group(1).strip(), _parse_list(m.group(2))
            for c in _alias_expand(col):
                intent["neq"].setdefault(c, [])
                intent["neq"][c].extend(vals)
            continue

        m = re.search(
            r"^\s*([\w\.\s]+?)\s+" + NOT_CONTAINS_PAT + r"\s+(.+)$",
            p,
            flags=re.IGNORECASE,
        )
        if m:
            col, vals = m.group(1).strip(), _parse_list(m.group(2))
            for c in _alias_expand(col):
                intent["not_contains"].setdefault(c, [])
                intent["not_contains"][c].extend(vals)
            continue

    settings = get_setting("DW_FTS_COLUMNS", scope="namespace") or {}
    fts_cols = settings.get("Contract") or settings.get("*", [])
    intent["_fts_columns"] = [c.strip().upper() for c in fts_cols]
    return intent
