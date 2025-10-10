# -*- coding: utf-8 -*-
"""
Parse /dw/rate comments:
  - fts: token1 | token2      -> OR operator + tokens
  - fts: token1 & token2      -> AND operator + tokens
  - eq: COL = VAL (ci, trim)  -> equality filter, flags optional
  - order_by: COL asc|desc
  - group_by: COL
  - gross: true|false
"""
import re
from typing import Dict, List, Tuple

_OR_SPLIT_RE = re.compile(r"\s+or\s+|\s*\|\s*|,", re.IGNORECASE)
_AND_SPLIT_RE = re.compile(r"\s*(?:&|\band\b)\s*", re.IGNORECASE)

_RE_FTS = re.compile(r"fts:\s*(?P<payload>.+?)(?:;|$)", re.IGNORECASE)
_RE_EQ  = re.compile(r"eq:\s*(?P<col>[^=]+?)\s*=\s*(?P<val>[^;]+?)(?:\((?P<flags>[^)]+)\))?(?:;|$)", re.IGNORECASE)
_RE_LIKE = re.compile(
    r"(contains|has|have):\s*(?P<col>[^=]+?)\s*=\s*(?P<val>[^;]+?)(?:\((?P<flags>[^)]+)\))?(?:;|$)",
    re.IGNORECASE,
)
_RE_ORDER = re.compile(r"order_by:\s*(?P<col>[A-Za-z0-9_ ]+)\s*(?P<dir>asc|desc)?", re.IGNORECASE)
_RE_GROUP = re.compile(r"group_by:\s*(?P<col>[A-Za-z0-9_ ]+)", re.IGNORECASE)
_RE_GROSS = re.compile(r"gross:\s*(?P<v>true|false)", re.IGNORECASE)


def _clean(s: str) -> str:
    s = (s or "").strip()
    # remove trailing punctuation commonly left
    return s.rstrip(".;, ")


def _strip_quotes(value: str) -> str:
    text = (value or "").strip()
    if len(text) >= 2 and text[0] in {'"', "'"} and text[-1] == text[0]:
        return text[1:-1]
    return text


def _split_values(payload: str) -> List[str]:
    values: List[str] = []
    for part in _OR_SPLIT_RE.split(payload or ""):
        cleaned = _clean(_strip_quotes(part))
        if cleaned:
            values.append(cleaned)
    return values


def parse_fts(payload: str) -> Tuple[List[List[str]], str]:
    pl = _clean(payload)
    if not pl:
        return [], "OR"
    # Treat explicit AND connectors when no OR separators are present
    if re.search(r"(?:&|\band\b)", pl, re.IGNORECASE) and not re.search(r"(?:\bor\b|\|)", pl, re.IGNORECASE):
        parts = [p.strip() for p in _AND_SPLIT_RE.split(pl) if p.strip()]
        return [[p] for p in parts], "AND"
    parts = _split_values(pl)
    return [[p] for p in parts], "OR"


def parse_flags(flag_str: str) -> Dict[str, bool]:
    flags = {"ci": False, "trim": False}
    if not flag_str:
        return flags
    for raw in flag_str.split(","):
        f = raw.strip().lower()
        if f in ("ci", "case_insensitive"):
            flags["ci"] = True
        elif f == "trim":
            flags["trim"] = True
    return flags


def parse_rate_comment(comment: str) -> Dict:
    out = {
        "fts_tokens": [],
        "fts_operator": None,
        "eq_filters": [],
        "order_by": None,
        "order_dir": None,
        "group_by": None,
        "gross": None
    }
    c = comment or ""
    m_fts = _RE_FTS.search(c)
    if m_fts:
        groups, op = parse_fts(m_fts.group("payload"))
        out["fts_tokens"] = groups
        out["fts_operator"] = op
    for m in _RE_EQ.finditer(c):
        col = _clean(m.group("col")).upper().replace(" ", "_")
        flags = parse_flags(m.group("flags") or "")
        values = _split_values(m.group("val") or "")
        for value in values:
            out["eq_filters"].append(
                {
                    "col": col,
                    "val": value,
                    "ci": flags["ci"],
                    "trim": flags["trim"],
                    "op": "eq",
                }
            )
    for m in _RE_LIKE.finditer(c):
        col = _clean(m.group("col")).upper().replace(" ", "_")
        flags = parse_flags(m.group("flags") or "")
        values = _split_values(m.group("val") or "")
        for value in values:
            pattern = value
            if pattern and not pattern.startswith("%") and not pattern.endswith("%"):
                pattern = f"%{pattern}%"
            out["eq_filters"].append(
                {
                    "col": col,
                    "val": pattern,
                    "ci": flags["ci"],
                    "trim": flags["trim"],
                    "op": "like",
                }
            )
    m_order = _RE_ORDER.search(c)
    if m_order:
        out["order_by"] = _clean(m_order.group("col")).upper().replace(" ", "_")
        out["order_dir"] = (m_order.group("dir") or "DESC").upper()
    m_group = _RE_GROUP.search(c)
    if m_group:
        out["group_by"] = _clean(m_group.group("col")).upper().replace(" ", "_")
    m_gross = _RE_GROSS.search(c)
    if m_gross:
        out["gross"] = True if m_gross.group("v").lower() == "true" else False
    return out
