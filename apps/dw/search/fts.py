from __future__ import annotations

"""Lightweight helpers for building FTS and equality WHERE clauses."""

from typing import Dict, Iterable, List, Optional, Sequence, Tuple


def _normalize(value: Optional[str]) -> str:
    """Return a trimmed string, guarding against ``None`` values."""

    return (value or "").strip()


def _normalize_columns(raw: Iterable[object]) -> List[str]:
    seen: set[str] = set()
    result: List[str] = []
    for col in raw or []:
        if not isinstance(col, str):
            continue
        text = col.strip().strip('"')
        if not text:
            continue
        upper = text.upper()
        if upper in seen:
            continue
        seen.add(upper)
        result.append(text)
    return result


def get_fts_engine(settings: Dict) -> str:
    """Return the configured FTS engine, defaulting to ``like``."""

    value = settings.get("DW_FTS_ENGINE") if isinstance(settings, dict) else None
    text = _normalize(str(value)) if value is not None else ""
    engine = (text or "like").lower()
    return engine or "like"


def get_fts_columns(table: str, settings: Dict) -> List[str]:
    """Look up configured FTS columns for ``table`` with wildcard fallback."""

    mapping = settings.get("DW_FTS_COLUMNS") if isinstance(settings, dict) else None
    if not isinstance(mapping, dict):
        return []

    table_key = (table or "").strip()
    candidates: Sequence[object] = []
    if table_key:
        stripped = table_key.strip('"')
        for key in (table_key, stripped, stripped.upper(), stripped.lower()):
            if key in mapping:
                candidates = mapping.get(key) or []
                break
        if not candidates:
            quoted = f'"{stripped}"'
            if quoted in mapping:
                candidates = mapping.get(quoted) or []
    if not candidates:
        candidates = mapping.get("*") or []
    return _normalize_columns(candidates)


def extract_fts_tokens(fts_groups: List[List[str]] | None, min_len: int = 2) -> List[List[str]]:
    """Filter token groups keeping tokens that meet the minimum length."""

    if min_len <= 1:
        min_len = 1
    output: List[List[str]] = []
    for group in fts_groups or []:
        cleaned: List[str] = []
        for token in group or []:
            text = _normalize(token)
            if len(text) >= min_len:
                cleaned.append(text)
        if cleaned:
            output.append(cleaned)
    return output


def _build_fulltext_where_like(
    columns: Sequence[str],
    groups: Sequence[Sequence[str]],
    *,
    operator: str,
    bind_prefix: str,
    start_index: int,
) -> Tuple[str, Dict[str, str], int]:
    binds: Dict[str, str] = {}
    clauses: List[str] = []
    index = max(0, int(start_index))

    normalized_columns = [col for col in (_normalize(col) for col in columns) if col]
    if not normalized_columns:
        return "", binds, index

    for group in groups:
        token_clauses: List[str] = []
        for token in group:
            token_text = _normalize(token)
            if not token_text:
                continue
            bind_name = f"{bind_prefix}{index}"
            binds[bind_name] = f"%{token_text}%"
            index += 1
            column_checks = [
                f"UPPER(NVL({column},'')) LIKE UPPER(:{bind_name})" for column in normalized_columns
            ]
            token_clauses.append("(" + " OR ".join(column_checks) + ")")
        if token_clauses:
            clauses.append("(" + " OR ".join(token_clauses) + ")")

    if not clauses:
        return "", binds, start_index

    joiner = " OR " if (operator or "").upper() != "AND" else " AND "
    return "(" + joiner.join(clauses) + ")", binds, index


def _build_fulltext_where_impl(
    engine: str,
    columns: Sequence[str],
    groups: Sequence[Sequence[str]],
    *,
    operator: str = "OR",
    bind_prefix: str = "fts_",
    start_index: int = 0,
) -> Tuple[str, Dict[str, str], int]:
    normalized_engine = (engine or "like").strip().lower() or "like"
    filtered_groups = [tuple(g) for g in groups if g]
    if not filtered_groups:
        return "", {}, start_index

    if normalized_engine in {"", "like", "oracle_text"}:
        sql, binds, next_index = _build_fulltext_where_like(
            columns,
            filtered_groups,
            operator=operator,
            bind_prefix=bind_prefix,
            start_index=start_index,
        )
        return sql, binds, next_index

    # Unsupported engines fall back to an empty predicate for now.
    return "", {}, start_index


def build_fulltext_where(*args, **kwargs):  # type: ignore[override]
    """Construct a WHERE fragment for full-text tokens.

    The function supports both the legacy signature used across the codebase::

        build_fulltext_where(groups, columns, engine="like", min_len=2)

    and the new explicit signature::

        build_fulltext_where("like", columns, groups, operator="OR", start_index=0)

    When called with the new signature the return value is ``(sql, binds, next_index)``.
    The legacy signature keeps returning ``(sql, binds)`` for backwards compatibility.
    """

    min_len = kwargs.pop("min_len", kwargs.pop("min_token_len", 2))
    min_len = max(1, int(min_len or 1))

    if args and isinstance(args[0], list):
        # Legacy signature: (groups, columns, engine="like", min_len=2, ...)
        groups = args[0]
        columns = args[1] if len(args) > 1 else kwargs.get("columns", [])
        engine = kwargs.get("engine", "like")
        operator = kwargs.get("group_operator", kwargs.get("operator", "OR"))
        bind_prefix = kwargs.get("bind_prefix", "fts_")
        bind_start = kwargs.get("bind_start", kwargs.get("start_index", 0))
        filtered_groups = extract_fts_tokens(groups, min_len=min_len)
        sql, binds, _ = _build_fulltext_where_impl(
            engine,
            columns,
            filtered_groups,
            operator=operator,
            bind_prefix=bind_prefix,
            start_index=int(bind_start or 0),
        )
        return sql, binds

    engine = args[0] if args else kwargs.get("engine", "like")
    columns = args[1] if len(args) > 1 else kwargs.get("columns", [])
    groups = args[2] if len(args) > 2 else kwargs.get("groups", [])
    operator = args[3] if len(args) > 3 else kwargs.get("operator", "OR")
    bind_prefix = args[4] if len(args) > 4 else kwargs.get("bind_prefix", "fts_")
    start_index = args[5] if len(args) > 5 else kwargs.get("start_index", 0)

    filtered_groups = extract_fts_tokens(groups, min_len=min_len)
    return _build_fulltext_where_impl(
        engine,
        columns,
        filtered_groups,
        operator=operator,
        bind_prefix=bind_prefix,
        start_index=int(start_index or 0),
    )


def resolve_fts_config(settings: Dict) -> Dict[str, object]:
    """Extract FTS configuration from a loose settings mapping."""

    engine = get_fts_engine(settings)
    columns_map = settings.get("DW_FTS_COLUMNS", {}) if isinstance(settings, dict) else {}
    min_len = settings.get("DW_FTS_MIN_TOKEN_LEN", 2) if isinstance(settings, dict) else 2
    try:
        min_len_int = int(min_len)
    except (TypeError, ValueError):  # pragma: no cover - defensive fallback
        min_len_int = 2

    return {
        "engine": engine if engine in {"like", "oracle_text"} else "like",
        "fts_columns_map": columns_map or {},
        "min_len": max(1, min_len_int),
    }


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
    "extract_fts_tokens",
    "get_fts_columns",
    "get_fts_engine",
    "resolve_fts_config",
]
