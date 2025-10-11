from __future__ import annotations

"""Builders for simple equality filters used by DW search endpoints."""

from typing import Dict, List, Tuple


def build_eq_clause(
    field: str,
    values: List[str],
    aliases: List[str] | None,
    bind_names: List[str],
) -> str:
    """Return a WHERE clause for equality filters on ``field``."""

    columns = [col.strip() for col in (aliases or [field]) if col and col.strip()]
    if not columns or not bind_names:
        return ""

    normalized_values = [value.strip() for value in values if value.strip()]
    if not normalized_values:
        return ""

    if len(bind_names) == 1:
        bind_expr = f"UPPER(TRIM(:{bind_names[0]}))"
        comparisons = [f"UPPER(TRIM({column})) = {bind_expr}" for column in columns]
    else:
        bind_list = ", ".join(f"UPPER(TRIM(:{name}))" for name in bind_names)
        comparisons = [f"UPPER(TRIM({column})) IN ({bind_list})" for column in columns]

    return "(" + " OR ".join(comparisons) + ")"


def build_eq_where(
    eq_filters: List[Dict],
    alias_map: Dict[str, List[str]],
    bind_prefix: str = "eq_",
    start_index: int = 0,
) -> Tuple[str, Dict[str, str], int]:
    """Aggregate equality filters using OR/IN semantics with alias expansion."""

    if not eq_filters:
        return "", {}, start_index

    grouped: Dict[str, List[str]] = {}
    for entry in eq_filters:
        if not isinstance(entry, dict):
            continue
        col = (entry.get("col") or entry.get("column") or "").strip()
        val = entry.get("val") if "val" in entry else entry.get("value")
        if not col or val is None:
            continue
        grouped.setdefault(col, []).append(str(val))

    if not grouped:
        return "", {}, start_index

    binds: Dict[str, str] = {}
    parts: List[str] = []
    idx = start_index

    for logical_col, values in grouped.items():
        unique_values: List[str] = []
        seen: set[str] = set()
        for value in values:
            norm = value.strip()
            if not norm:
                continue
            upper = norm.upper()
            if upper in seen:
                continue
            seen.add(upper)
            unique_values.append(norm)

        if not unique_values:
            continue

        bind_names: List[str] = []
        for value in unique_values:
            bind_name = f"{bind_prefix}{idx}"
            idx += 1
            binds[bind_name] = value
            bind_names.append(bind_name)

        clause = build_eq_clause(
            logical_col,
            unique_values,
            alias_map.get(logical_col) or [logical_col],
            bind_names,
        )
        if clause:
            parts.append(clause)

    where_sql = " AND ".join(parts) if parts else ""
    return where_sql, binds, idx


__all__ = ["build_eq_clause", "build_eq_where"]
