from __future__ import annotations

"""Builders for simple equality filters used by DW search endpoints."""

from typing import Dict, List, Tuple


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

        physical_cols = alias_map.get(logical_col) or [logical_col]
        normalized_cols = [col.strip() for col in physical_cols if col and col.strip()]
        if not normalized_cols:
            continue

        bind_names: List[str] = []
        for value in unique_values:
            bind_name = f"{bind_prefix}{idx}"
            idx += 1
            binds[bind_name] = value
            bind_names.append(f":{bind_name}")

        if not bind_names:
            continue

        in_clause = ", ".join(f"UPPER(TRIM({name}))" for name in bind_names)
        column_checks = [
            f"UPPER(TRIM({column})) IN ({in_clause})" for column in normalized_cols
        ]
        parts.append("(" + " OR ".join(column_checks) + ")")

    where_sql = " AND ".join(parts) if parts else ""
    return where_sql, binds, idx


__all__ = ["build_eq_where"]
