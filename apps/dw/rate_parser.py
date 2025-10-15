from __future__ import annotations

import re
from typing import Any, Dict, List

try:  # pragma: no cover - optional dependency during tests
    from apps.dw.settings import get_setting
except Exception:  # pragma: no cover - fallback when settings helper unavailable
    def get_setting(*_args, **kwargs):  # type: ignore[return-type]
        return kwargs.get("default")

_OR_SPLIT = re.compile(r"\s+or\s+", re.IGNORECASE)

NEG_EQ_PAT = r"(?:!=|<>|≠|\bne\b|not\s*=|not\s*equal(?:\s*to)?|does\s*not\s*equal|doesn['’]?t\s*equal|is\s*not|لا\s*يساوي|مش\s*مساوي|غير|ماه?وش)"
NOT_CONTAINS_PAT = r"(?:does\s*not\s*(?:contain|have|has|include)|not\s*(?:like|contain|contains|have|has|include)|doesn['’]?t\s*(?:contain|have|has|include)|exclude|excludes|excluding|without|لا\s*يحتوي|مش\s*فيه|مافيهوش|بدون)"
IS_EMPTY_PAT = r"(?:is\s*empty|is\s*null|=\s*''\s*|فارغ|خالي|NULL)"
IS_NOT_EMPTY_PAT = r"(?:is\s*not\s*empty|is\s*not\s*null|<>\s*''\s*|مش\s*فاضي|غير\s*فارغ)"


def _split_directives(comment: str) -> List[str]:
    return [p.strip() for p in (comment or "").split(";") if p.strip()]


def _parse_list(val: str) -> List[str]:
    if " or " in val.lower():
        parts = _OR_SPLIT.split(val)
    else:
        parts = [p.strip() for p in re.split(r",", val) if p.strip()]
    return [p.strip() for p in parts if p.strip()]


def parse_rate_comment(comment: str) -> Dict[str, Any]:
    intent: Dict[str, Any] = {
        "fts_groups": [],
        "eq": {},
        "neq": {},
        "contains": {},
        "not_contains": {},
        "empty_any": [],
        "empty_all": [],
        "not_empty": [],
        "not_contains_tokens": {},
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
                col_key = col.strip().upper()
                intent["eq"].setdefault(col_key, [])
                intent["eq"][col_key].extend(vals)
            continue
        if any(low.startswith(prefix) for prefix in ("neq:", "ne:")):
            expr = p.split(":", 1)[1]
            m = re.match(
                r"\s*([^=<>]+?)\s*(?:!=|<>|=)\s*(.+)$", expr.strip(), flags=re.IGNORECASE
            )
            if m:
                col, vals = m.group(1).strip(), _parse_list(m.group(2))
                col_key = col.strip().upper()
                intent["neq"].setdefault(col_key, [])
                intent["neq"][col_key].extend(vals)
            continue
        if low.startswith("contains:"):
            expr = p.split(":", 1)[1]
            m = re.match(r"\s*([^=]+?)\s*=\s*(.+)$", expr.strip(), flags=re.IGNORECASE)
            if m:
                col, vals = m.group(1).strip(), _parse_list(m.group(2))
                col_key = col.strip().upper()
                intent["contains"].setdefault(col_key, [])
                intent["contains"][col_key].extend(vals)
            continue
        not_contains_prefixes = (
            "not_contains:",
            "not contains:",
            "not-like:",
            "not_like:",
            "not_has:",
            "not has:",
            "doesnt contain:",
            "doesn't contain:",
            "doesnt have:",
            "doesn't have:",
            "exclude:",
        )
        if any(low.startswith(prefix) for prefix in not_contains_prefixes):
            expr = p.split(":", 1)[1]
            m = re.match(r"\s*([^=]+?)\s*=\s*(.+)$", expr.strip(), flags=re.IGNORECASE)
            if m:
                col, vals = m.group(1).strip(), _parse_list(m.group(2))
                col_key = col.strip().upper()
                cleaned = [v for v in vals if v]
                if cleaned:
                    intent["not_contains"].setdefault(col_key, [])
                    intent["not_contains"][col_key].extend(cleaned)
                    tokens = [token.strip() for token in cleaned if token.strip()]
                    if tokens:
                        intent["not_contains_tokens"].setdefault(col_key, [])
                        intent["not_contains_tokens"][col_key].extend(tokens)
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
                intent["empty_any"].append([c.upper()])
            continue
        if low.startswith("not_empty:"):
            cols = [c.strip() for c in p.split(":", 1)[1].split(",") if c.strip()]
            for c in cols:
                intent["not_empty"].append(c.upper())
            continue
        if low.startswith("order_by:"):
            intent["order_by"] = p.split(":", 1)[1].strip()
            continue

        m = re.search(
            r"^\s*([\w\.\s]+?)\s+" + IS_EMPTY_PAT + r"\s*$", p, flags=re.IGNORECASE
        )
        if m:
            intent["empty_any"].append([m.group(1).strip().upper()])
            continue
        m = re.search(
            r"^\s*([\w\.\s]+?)\s+" + IS_NOT_EMPTY_PAT + r"\s*$", p, flags=re.IGNORECASE
        )
        if m:
            intent["not_empty"].append(m.group(1).strip().upper())
            continue

        m = re.search(
            r"^\s*([\w\.\s]+?)\s+" + NEG_EQ_PAT + r"\s+(.+)$", p, flags=re.IGNORECASE
        )
        if m:
            col, vals = m.group(1).strip(), _parse_list(m.group(2))
            col_key = col.strip().upper()
            intent["neq"].setdefault(col_key, [])
            intent["neq"][col_key].extend(vals)
            continue

        m = re.search(
            r"^\s*([\w\.\s]+?)\s+" + NOT_CONTAINS_PAT + r"\s+(.+)$",
            p,
            flags=re.IGNORECASE,
        )
        if m:
            col, vals = m.group(1).strip(), _parse_list(m.group(2))
            col_key = col.strip().upper()
            cleaned = [v for v in vals if v]
            if cleaned:
                intent["not_contains"].setdefault(col_key, [])
                intent["not_contains"][col_key].extend(cleaned)
                tokens = [token.strip() for token in cleaned if token.strip()]
                if tokens:
                    intent["not_contains_tokens"].setdefault(col_key, [])
                    intent["not_contains_tokens"][col_key].extend(tokens)
            continue

    settings = get_setting("DW_FTS_COLUMNS", scope="namespace") or {}
    fts_cols = settings.get("Contract") or settings.get("*", [])
    intent["_fts_columns"] = [c.strip().upper() for c in fts_cols]
    return intent
