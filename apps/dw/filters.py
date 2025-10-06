"""Filter helpers for DW intents."""

from __future__ import annotations

from typing import Dict, List, Tuple

try:  # pragma: no cover - optional dependency
    from apps.dw.settings_util import get_setting
except Exception:  # pragma: no cover - fallback when settings backend missing
    def get_setting(key: str, *, scope=None, namespace=None, default=None):
        return default

__all__ = [
    "eq_filters_to_where",
    "request_type_synonyms",
]


def _allowlist_columns() -> List[str]:
    cols = get_setting("DW_EXPLICIT_FILTER_COLUMNS", scope="namespace", namespace="dw::common")
    if not isinstance(cols, list):
        return []
    return [str(col).strip().upper() for col in cols if isinstance(col, str) and col.strip()]


def _normalize_col_name(col: str) -> str:
    return col.strip().upper().replace(" ", "_")


def _apply_flags(expr_col: str, bind_name: str, ci: bool, trim: bool) -> str:
    column_sql = expr_col
    bind_sql = f":{bind_name}"
    if trim:
        column_sql = f"TRIM({column_sql})"
        bind_sql = f"TRIM({bind_sql})"
    if ci:
        column_sql = f"UPPER({column_sql})"
        bind_sql = f"UPPER({bind_sql})"
    return f"{column_sql} = {bind_sql}"


def eq_filters_to_where(eq_filters: List[Dict]) -> Tuple[str, Dict[str, str]]:
    """Convert parsed equality filters to a SQL fragment and bind map."""

    if not eq_filters:
        return "", {}

    allow = set(_allowlist_columns())
    clauses: List[str] = []
    binds: Dict[str, str] = {}

    for idx, filt in enumerate(eq_filters):
        raw_col = str(filt.get("col", ""))
        col = _normalize_col_name(raw_col)
        if allow and col not in allow:
            continue
        val = str(filt.get("val", ""))
        if not val:
            continue
        ci = bool(filt.get("ci", False))
        trim = bool(filt.get("trim", False))
        bind_name = f"eq_{idx}"
        binds[bind_name] = val
        clauses.append(_apply_flags(col, bind_name, ci, trim))

    if not clauses:
        return "", {}

    where_sql = " AND ".join(f"({clause})" for clause in clauses)
    return where_sql, binds


def request_type_synonyms(values: List[str]) -> List[str]:
    """Expand REQUEST_TYPE synonyms using configuration."""

    synonyms = get_setting("DW_ENUM_SYNONYMS", scope="namespace", namespace="dw::common") or {}
    domain = {}
    if isinstance(synonyms, dict):
        domain = synonyms.get("Contract.REQUEST_TYPE") or {}
    out: List[str] = []
    for value in values:
        key = str(value or "").strip().lower()
        if not key:
            continue
        if isinstance(domain, dict) and key in domain:
            equals = domain[key].get("equals") if isinstance(domain[key], dict) else None
            if isinstance(equals, list):
                for candidate in equals:
                    upper = str(candidate or "").strip().upper()
                    if upper and upper not in out:
                        out.append(upper)
                continue
        upper = str(value).strip().upper()
        if upper and upper not in out:
            out.append(upper)
    return out
