from __future__ import annotations
"""
Human-friendly explain builder for DW responses.
Keeps a consistent, terse style with all the useful knobs.
"""
from typing import Any, Dict, List


def _fmt_tokens(tokens: Any) -> str:
    if not tokens:
        return "[]"
    if isinstance(tokens, list) and tokens and isinstance(tokens[0], list):
        return " | ".join(" & ".join(group) for group in tokens)
    if isinstance(tokens, (list, tuple, set)):
        return " | ".join(str(token) for token in tokens)
    return str(tokens)


def build_explain(meta: Dict[str, Any]) -> str:
    """Compose a compact explanation string from meta payload."""

    parts: List[str] = []
    meta = meta or {}

    fts_meta = meta.get("fts") or meta.get("FTS") or {}
    if isinstance(fts_meta, dict) and fts_meta.get("enabled"):
        operator = "AND" if fts_meta.get("operator") == "AND" else "OR"
        tokens = _fmt_tokens(fts_meta.get("tokens"))
        columns = fts_meta.get("columns") or []
        parts.append(f"FTS={operator} on {len(columns)} cols; tokens=({tokens})")

    eq_filters = meta.get("eq_filters") or meta.get("intent", {}).get("eq_filters") or []
    filter_lines: List[str] = []
    for entry in eq_filters or []:
        if not isinstance(entry, dict):
            continue
        column = entry.get("col") or entry.get("column")
        value = entry.get("val") or entry.get("value")
        if not column:
            continue
        flags: List[str] = []
        if entry.get("ci"):
            flags.append("ci")
        if entry.get("trim"):
            flags.append("trim")
        extra = f" ({','.join(flags)})" if flags else ""
        filter_lines.append(f"{column} = {value}{extra}")
    if filter_lines:
        parts.append("Filters: " + "; ".join(filter_lines))

    group_by = meta.get("group_by") or meta.get("intent", {}).get("group_by")
    if group_by:
        parts.append(f"Group by: {group_by}")

    gross_flag = meta.get("gross")
    if gross_flag is None:
        gross_flag = meta.get("intent", {}).get("gross")
    if gross_flag is not None:
        parts.append("Measure: GROSS" if gross_flag else "Measure: NET")

    date_start = meta.get("date_start") or meta.get("binds", {}).get("date_start")
    date_end = meta.get("date_end") or meta.get("binds", {}).get("date_end")
    if date_start or date_end:
        parts.append(f"Window: {date_start}..{date_end}")

    sort_by = meta.get("sort_by") or meta.get("intent", {}).get("sort_by")
    sort_desc = meta.get("sort_desc")
    if sort_desc is None:
        sort_desc = meta.get("intent", {}).get("sort_desc")
    if sort_by:
        direction = "DESC" if sort_desc else "ASC"
        parts.append(f"Order by: {sort_by} {direction}")

    strategy = meta.get("strategy")
    if strategy:
        parts.append(f"Strategy: {strategy}")

    if not parts:
        return "Plan constructed."
    return " | ".join(parts)

