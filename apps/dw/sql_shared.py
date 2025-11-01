from __future__ import annotations

from typing import Any, Dict, Iterable, List, Tuple

try:  # pragma: no cover - optional dependency in tests
    from sqlalchemy import create_engine, text
except Exception:  # pragma: no cover - allow tests without SQLAlchemy
    create_engine = None  # type: ignore[assignment]
    text = None  # type: ignore[assignment]

from apps.dw.settings import get_setting

OracleBlank = "''"


def get_engine():
    """Return a SQLAlchemy engine configured for the DW datasource."""

    url = get_setting("APP_DB_URL")
    if not url:
        raise RuntimeError("APP_DB_URL setting is required for DW SQL execution")
    if create_engine is None:  # pragma: no cover - helpful message when dependency missing
        raise RuntimeError("sqlalchemy is required to execute DW SQL queries")
    return create_engine(url, pool_pre_ping=True, future=True)


def dw_table() -> str:
    return get_setting("DW_CONTRACT_TABLE") or "Contract"


def dw_date_col() -> str:
    return get_setting("DW_DATE_COLUMN") or "REQUEST_DATE"


def fts_engine() -> str:
    engine = get_setting("DW_FTS_ENGINE") or "like"
    return str(engine).strip().lower() or "like"


def fts_columns_for(table: str) -> List[str]:
    mapping = get_setting("DW_FTS_COLUMNS") or {}
    columns: Iterable[str] = mapping.get(table) or mapping.get("*", []) or []
    cleaned: List[str] = []
    seen: set[str] = set()
    for col in columns:
        if not isinstance(col, str):
            continue
        text = col.strip()
        if not text:
            continue
        upper = text.upper()
        if upper in seen:
            continue
        seen.add(upper)
        cleaned.append(upper)
    return cleaned


def explicit_columns() -> List[str]:
    columns = get_setting("DW_EXPLICIT_FILTER_COLUMNS") or []
    cleaned: List[str] = []
    for col in columns:
        if not isinstance(col, str):
            continue
        text = col.strip()
        if not text:
            continue
        cleaned.append(text.upper())
    return cleaned


def eq_alias_columns() -> Dict[str, List[str]]:
    mapping = get_setting("DW_EQ_ALIAS_COLUMNS") or {}
    normalized: Dict[str, List[str]] = {}
    for key, cols in mapping.items():
        if not isinstance(cols, Iterable):
            continue
        bucket: List[str] = []
        for col in cols:
            if not isinstance(col, str):
                continue
            text = col.strip()
            if not text:
                continue
            bucket.append(text.upper())
        if bucket:
            normalized[str(key).strip().upper()] = bucket
    return normalized


def request_type_synonyms() -> Dict[str, Dict[str, Dict[str, List[str]]]]:
    raw = get_setting("DW_ENUM_SYNONYMS") or {}
    if not isinstance(raw, dict):
        return {}
    return raw


def normalize_ident(value: str) -> str:
    return (value or "").strip().upper()


def in_list_sql(col: str, bind_names: Iterable[str], upper_trim: bool = True) -> str:
    target = f"UPPER(TRIM({col}))" if upper_trim else col
    parts = []
    for name in bind_names:
        if upper_trim:
            parts.append(f"UPPER(:{name})")
        else:
            parts.append(f":{name}")
    csv = ", ".join(parts)
    return f"{target} IN ({csv})"


def equals_sql(col: str, bind_name: str, upper_trim: bool = True) -> str:
    target = f"UPPER(TRIM({col}))" if upper_trim else col
    expr = f"UPPER(:{bind_name})" if upper_trim else f":{bind_name}"
    return f"{target} = {expr}"


def not_equals_sql(col: str, bind_name: str, upper_trim: bool = True) -> str:
    target = f"UPPER(TRIM({col}))" if upper_trim else col
    expr = f"UPPER(:{bind_name})" if upper_trim else f":{bind_name}"
    return f"{target} <> {expr}"


def like_sql(
    col: str,
    bind_name: str,
    *,
    negate: bool = False,
    nvl: bool = True,
    upper: bool = True,
) -> str:
    base = col
    if nvl:
        base = f"NVL({base},{OracleBlank})"
    if upper:
        base = f"UPPER({base})"
    comparator = "NOT LIKE" if negate else "LIKE"
    value_expr = f"UPPER(:{bind_name})" if upper else f":{bind_name}"
    return f"{base} {comparator} {value_expr}"


def is_empty_sql(col: str) -> str:
    return f"TRIM(NVL({col},{OracleBlank})) = ''"


def not_empty_sql(col: str) -> str:
    return f"TRIM(NVL({col},{OracleBlank})) <> ''"


def or_join(parts: Iterable[str]) -> str:
    filtered = [part for part in parts if part]
    return "(" + " OR ".join(filtered) + ")" if filtered else ""


def and_join(parts: Iterable[str]) -> str:
    filtered = [part for part in parts if part]
    return "(" + " AND ".join(filtered) + ")" if filtered else ""


def exec_sql(sql: str, binds: Dict[str, Any]) -> Tuple[List[str], List[List[Any]]]:
    if text is None:  # pragma: no cover - dependency guard
        raise RuntimeError("sqlalchemy is required to execute DW SQL queries")
    engine = get_engine()
    with engine.connect() as connection:
        result = connection.execute(text(sql), binds or {})
        columns = list(result.keys())
        rows = [list(row) for row in result.fetchall()]
    return columns, rows


__all__ = [
    "OracleBlank",
    "and_join",
    "dw_date_col",
    "dw_table",
    "eq_alias_columns",
    "exec_sql",
    "explicit_columns",
    "fts_columns_for",
    "fts_engine",
    "get_engine",
    "in_list_sql",
    "is_empty_sql",
    "like_sql",
    "normalize_ident",
    "not_empty_sql",
    "not_equals_sql",
    "or_join",
    "request_type_synonyms",
]
