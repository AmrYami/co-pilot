from __future__ import annotations

"""Lightweight helpers for building FTS and boolean group WHERE clauses."""

from typing import Dict, List, Optional, Tuple


def _normalize(value: Optional[str]) -> str:
    """Return a trimmed string, guarding against ``None`` values."""

    return (value or "").strip()


def resolve_fts_config(settings: Dict) -> Dict[str, object]:
    """Extract FTS configuration from a loose settings mapping."""

    engine = _normalize(settings.get("DW_FTS_ENGINE", "like")).lower()
    if engine not in {"like", "oracle_text"}:
        engine = "like"

    columns_map = settings.get("DW_FTS_COLUMNS", {}) or {}
    min_len = settings.get("DW_FTS_MIN_TOKEN_LEN", 2)
    try:
        min_len_int = int(min_len)
    except (TypeError, ValueError):  # pragma: no cover - defensive fallback
        min_len_int = 2

    return {
        "engine": engine,
        "fts_columns_map": columns_map,
        "min_len": max(1, min_len_int),
    }


def build_fulltext_where(
    groups: List[List[str]],
    columns: List[str],
    engine: str = "like",
    min_len: int = 2,
    *,
    bind_prefix: str = "fts_",
    bind_start: int = 0,
    group_operator: str = "OR",
) -> Tuple[str, Dict[str, str]]:
    """Construct a ``LIKE`` predicate for the provided token groups."""

    if not engine:
        return "", {}

    filtered_groups: List[List[str]] = []
    for group in groups or []:
        cleaned = [token for token in group if len(token.strip()) >= min_len]
        if cleaned:
            filtered_groups.append(cleaned)

    if not filtered_groups or not columns:
        return "", {}

    binds: Dict[str, str] = {}
    clauses: List[str] = []
    bind_index = bind_start

    for group in filtered_groups:
        token_clauses: List[str] = []
        for token in group:
            bind_name = f"{bind_prefix}{bind_index}"
            bind_index += 1
            binds[bind_name] = f"%{token}%"
            column_checks = [
                f"UPPER(NVL({column},'')) LIKE UPPER(:{bind_name})" for column in columns
            ]
            token_clauses.append("(" + " OR ".join(column_checks) + ")")
        if token_clauses:
            clauses.append("(" + " OR ".join(token_clauses) + ")")

    if not clauses:
        return "", {}

    joiner = " AND " if group_operator.upper() == "AND" else " OR "
    return "(" + joiner.join(clauses) + ")", binds


def build_boolean_groups_where(
    boolean_groups: List[Dict],
    eq_alias_map: Dict[str, List[str]],
    *,
    bind_prefix: str = "eq_bg_",
    bind_start: int = 0,
) -> Tuple[str, Dict[str, str]]:
    """Render equality boolean groups into a SQL fragment."""

    if not boolean_groups:
        return "", {}

    binds: Dict[str, str] = {}
    clauses: List[str] = []
    bind_index = bind_start

    for group in boolean_groups:
        raw_fields = group.get("fields") if isinstance(group, dict) else None
        if not isinstance(raw_fields, list):
            continue

        field_clauses: List[str] = []
        for entry in raw_fields:
            if not isinstance(entry, dict):
                continue

            field_name = _normalize(entry.get("field"))
            if not field_name:
                continue

            values = entry.get("values") or []
            cleaned_values: List[str] = []
            seen_values: set[str] = set()
            for value in values:
                text = _normalize(str(value))
                if not text:
                    continue
                key = text.upper()
                if key in seen_values:
                    continue
                seen_values.add(key)
                cleaned_values.append(text)

            if not cleaned_values:
                continue

            columns = eq_alias_map.get(field_name) or [field_name]
            normalized_columns = [col.strip() for col in columns if col and col.strip()]
            if not normalized_columns:
                continue

            bind_names: List[str] = []
            for value in cleaned_values:
                bind_name = f"{bind_prefix}{bind_index}"
                bind_index += 1
                binds[bind_name] = value
                bind_names.append(bind_name)

            if not bind_names:
                continue

            in_list = ", ".join(f"UPPER(TRIM(:{name}))" for name in bind_names)
            column_checks = [
                f"UPPER(TRIM({column})) IN ({in_list})" for column in normalized_columns
            ]
            field_clauses.append("(" + " OR ".join(column_checks) + ")")

        if field_clauses:
            clauses.append("(" + " AND ".join(field_clauses) + ")")

    if not clauses:
        return "", {}

    return " AND ".join(clauses), binds


__all__ = [
    "build_boolean_groups_where",
    "build_fulltext_where",
    "resolve_fts_config",
]
