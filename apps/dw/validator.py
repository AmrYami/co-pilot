from __future__ import annotations

import re
from typing import Dict, Iterable, List, Optional

_RE_FENCE = re.compile(r"```(?:sql)?\s*(.+?)```", re.IGNORECASE | re.DOTALL)
_RE_START = re.compile(r"^\s*(SELECT|WITH)\b", re.IGNORECASE | re.DOTALL)
_RE_FIRST_SELECT = re.compile(r"(SELECT|WITH)\b.*", re.IGNORECASE | re.DOTALL)
_RE_DML = re.compile(
    r"\b(INSERT|UPDATE|DELETE|MERGE|DROP|ALTER|TRUNCATE|CREATE|GRANT|REVOKE)\b",
    re.IGNORECASE,
)
_RE_BINDS = re.compile(r":([a-zA-Z_][a-zA-Z0-9_]*)")

WHITELIST_BINDS = {
    "date_start",
    "date_end",
    "top_n",
    "owner_name",
    "dept",
    "entity_no",
    "contract_id_pattern",
    "request_type",
}


def extract_sql(text: str) -> str:
    """Extract SQL content, preferring fenced blocks."""

    if not text:
        return ""
    match = _RE_FENCE.search(text)
    if match:
        return match.group(1).strip()
    match = _RE_FIRST_SELECT.search(text)
    if match:
        return match.group(0).strip()
    return ""


def analyze_binds(sql: str) -> List[str]:
    if not sql:
        return []
    seen: set[str] = set()
    ordered: List[str] = []
    for name in _RE_BINDS.findall(sql):
        lowered = name.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        ordered.append(name)
    return ordered


def basic_checks(sql: str, allowed_binds: Optional[Iterable[str]] = None) -> Dict[str, object]:
    """Basic validation ensuring SQL is a SELECT/CTE with approved binds only."""

    errs: List[str] = []
    cleaned = (sql or "").strip()
    if not cleaned or not _RE_START.match(cleaned):
        errs.append("not_select")
        return {"ok": False, "errors": errs, "binds": []}
    if _RE_DML.search(cleaned):
        errs.append("forbidden_dml")
    binds = analyze_binds(cleaned)
    whitelist = set((allowed_binds or WHITELIST_BINDS) or [])
    illegal: List[str] = []
    for name in binds:
        lowered = name.lower()
        if lowered in whitelist:
            continue
        if lowered.startswith("eq_bg_"):
            continue
        illegal.append(lowered)
    if illegal:
        errs.append(f"illegal_binds:{','.join(illegal)}")
    binds_lower = [name.lower() for name in binds]
    return {"ok": len(errs) == 0, "errors": errs, "binds": binds_lower, "bind_names": binds}


def validate_sql(
    sql_text: str,
    allow_tables: Optional[Iterable[str]] = None,
    bind_whitelist: Optional[Iterable[str]] = None,
) -> Dict[str, object]:
    """Compatibility shim around :func:`basic_checks`."""

    _ = allow_tables  # retained for backwards compatibility
    return basic_checks(sql_text or "", allowed_binds=bind_whitelist)


__all__ = [
    "WHITELIST_BINDS",
    "extract_sql",
    "analyze_binds",
    "basic_checks",
    "validate_sql",
]
