from __future__ import annotations

from typing import Dict, List, Tuple


def _normalize_column(name: str) -> str:
    """Return an uppercase, underscore-safe representation of a column name."""
    return (name or "").strip().upper().replace(" ", "_")


def build_eq_where(
    eq_filters: List[Dict],
    allowed_columns: List[str],
) -> Tuple[str, Dict[str, str], List[Dict]]:
    """Build an equality WHERE fragment from structured filters."""
    allowed = {_normalize_column(col) for col in (allowed_columns or [])}
    allowed.add("REPRESENTATIVE_EMAIL")

    clauses: List[str] = []
    binds: Dict[str, str] = {}
    applied: List[Dict] = []
    seen: set[tuple[str, str]] = set()
    bind_idx = 0

    for raw in eq_filters or []:
        col = _normalize_column(raw.get("col"))
        value = raw.get("val")
        if not col or value is None:
            continue
        if col not in allowed:
            continue
        key = (col, str(value).strip())
        if key in seen:
            continue
        seen.add(key)

        ci = bool(raw.get("ci"))
        trim = bool(raw.get("trim"))
        bind_name = f"eq_{bind_idx}"
        bind_idx += 1

        lhs = col
        rhs = f":{bind_name}"
        if trim:
            lhs = f"TRIM({lhs})"
            rhs = f"TRIM(:{bind_name})"
        if ci:
            lhs = f"UPPER({lhs})"
            rhs = f"UPPER({rhs})"

        clauses.append(f"{lhs} = {rhs}")
        binds[bind_name] = str(value).strip()
        applied.append({"col": col, "bind": bind_name, "ci": ci, "trim": trim})

    return " AND ".join(clauses), binds, applied
