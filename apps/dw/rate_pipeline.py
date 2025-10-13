from __future__ import annotations

import logging
from typing import Any, Dict, List, Tuple

from apps.dw.rate_parser import parse_rate_comment
from apps.dw.sql_builders_rate import (
    build_empty_all,
    build_empty_any,
    build_fts_like,
    build_in_any_alias,
    build_neq_all,
    build_not_empty_all,
    build_not_like_all,
    qn,
)
from apps.dw.sql_exec_shared import execute_select, get_engine_for_default_datasource

try:  # pragma: no cover - optional dependency during tests
    from apps.dw.settings import get_setting
except Exception:  # pragma: no cover - fallback when settings helper unavailable
    def get_setting(*_args, **kwargs):  # type: ignore[return-type]
        return kwargs.get("default")


logger = logging.getLogger("dw.rate")


def _request_type_synonyms() -> Dict[str, Dict[str, List[str]]]:
    raw = get_setting("DW_ENUM_SYNONYMS", scope="namespace") or {}
    if not isinstance(raw, dict):
        return {}
    mapping = raw.get("Contract.REQUEST_TYPE", {})
    return mapping if isinstance(mapping, dict) else {}


def build_select_all(table: str, order_by: str | None = None, desc: bool = True) -> str:
    base = f'SELECT * FROM "{table}"'
    if not order_by:
        return base
    direction = "DESC" if desc else "ASC"
    return f"{base}\nORDER BY {order_by} {direction}"


def _clean_values(values: List[Any]) -> List[str]:
    cleaned: List[str] = []
    for value in values or []:
        if value is None:
            continue
        if isinstance(value, str):
            val = value.strip()
            if val:
                cleaned.append(val)
        else:
            cleaned.append(str(value))
    return cleaned


def _normalize_alias_map(raw_map: Any) -> Dict[str, List[str]]:
    normalized: Dict[str, List[str]] = {}
    if isinstance(raw_map, dict):
        for key, cols in raw_map.items():
            if not isinstance(cols, list):
                continue
            norm_key = str(key).strip().upper()
            norm_cols = [str(col).strip() for col in cols if str(col).strip()]
            if norm_cols:
                normalized[norm_key] = [c.upper() for c in norm_cols]
    return normalized


def _normalize_columns(columns: Any) -> List[str]:
    if not isinstance(columns, list):
        return []
    result: List[str] = []
    for col in columns:
        if col is None:
            continue
        text = str(col).strip()
        if text:
            result.append(text.upper())
    return result


def _ensure_column_list(columns: List[Any]) -> List[str]:
    cleaned: List[str] = []
    for col in columns or []:
        if col is None:
            continue
        text = str(col).strip()
        if text:
            cleaned.append(text.upper())
    return cleaned


def _build_like_any(col: str, values: List[str], bind_prefix: str, bind_seq: List[Tuple[str, str]]) -> str:
    parts: List[str] = []
    for value in values:
        name = f"{bind_prefix}_{len(bind_seq)}"
        bind_seq.append((name, f"%{value}%"))
        parts.append(f"{qn(col)} LIKE UPPER(:{name})")
    return "(" + " OR ".join(parts) + ")"


def _resolve_fts_columns(settings: Dict[str, Any], table: str = "Contract") -> List[str]:
    config = settings.get("DW_FTS_COLUMNS")
    if not isinstance(config, dict):
        return []
    candidates = config.get(table)
    if not candidates:
        candidates = config.get("*", [])
    return _normalize_columns(candidates)


def _get_bool_setting(key: str, default: bool = False) -> bool:
    truthy = {"1", "true", "t", "yes", "y", "on"}
    falsy = {"0", "false", "f", "no", "n", "off"}

    for scope in ("namespace", "global"):
        value = get_setting(key, scope=scope, default=None)
        if value is None:
            continue
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        text = str(value).strip().lower()
        if not text:
            continue
        if text in truthy:
            return True
        if text in falsy:
            return False
        return default

    return default


def build_where_sql(intent: Dict[str, Any], settings: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
    eq_alias_map = _normalize_alias_map(settings.get("DW_EQ_ALIAS_COLUMNS"))
    fts_columns = _resolve_fts_columns(settings)
    fts_engine = str(settings.get("DW_FTS_ENGINE") or "like").lower()

    clauses: List[str] = []
    bind_seq: List[Tuple[str, str]] = []

    eq_filters = intent.get("eq_filters")
    if eq_filters is None:
        eq_filters = [
            {"col": col, "values": values}
            for col, values in intent.get("eq", {}).items()
        ]
    for flt in eq_filters:
        col = flt.get("col")
        if not col:
            continue
        values = _clean_values(flt.get("values", []))
        if not values:
            continue
        col_key = str(col).strip().upper()
        clauses.append(build_in_any_alias(col_key, values, eq_alias_map, "eq", bind_seq))

    neq_filters = intent.get("neq_filters")
    if neq_filters is None:
        neq_filters = [
            {"col": col, "values": values}
            for col, values in intent.get("neq", {}).items()
        ]
    for flt in neq_filters:
        col = flt.get("col")
        if not col:
            continue
        values = _clean_values(flt.get("values", []))
        if not values:
            continue
        col_key = str(col).strip().upper()
        clauses.append(build_neq_all(col_key, values, "neq", bind_seq))

    contains_map = intent.get("contains", {}) or {}
    for col, values in contains_map.items():
        cleaned = _clean_values(values)
        if not cleaned:
            continue
        col_key = str(col).strip().upper()
        clauses.append(_build_like_any(col_key, cleaned, "like", bind_seq))

    tokens_map_raw = intent.get("not_contains_tokens", {}) or {}
    tokens_map = {
        str(key).strip().upper(): _clean_values(tokens)
        for key, tokens in tokens_map_raw.items()
    }
    for col, values in (intent.get("not_contains", {}) or {}).items():
        col_key = str(col).strip().upper()
        base_tokens = tokens_map.get(col_key) or _clean_values(values)
        if not base_tokens:
            continue
        clauses.append(build_not_like_all(col_key, base_tokens, "nlike", bind_seq))

    for cols in intent.get("empty_any", []) or []:
        column_list = _ensure_column_list(cols)
        if column_list:
            clauses.append(build_empty_any(column_list))

    for cols in intent.get("empty_all", []) or []:
        column_list = _ensure_column_list(cols)
        if column_list:
            clauses.append(build_empty_all(column_list))

    not_empty_cols = _ensure_column_list(intent.get("not_empty", []))
    if not_empty_cols:
        clauses.append(build_not_empty_all(not_empty_cols))

    fts_groups_raw: List[List[str]] = intent.get("fts_groups", []) or []
    if fts_engine == "like" and fts_columns and fts_groups_raw:
        normalized_groups: List[List[str]] = []
        for group in fts_groups_raw:
            tokens = [token.strip() for token in group if isinstance(token, str) and token.strip()]
            if tokens:
                normalized_groups.append(tokens)
        if normalized_groups:
            clauses.append(
                build_fts_like(normalized_groups, fts_columns, "fts", bind_seq, groups_op="OR")
            )

    where_sql = " AND ".join(clauses) if clauses else "1=1"
    binds = {name: value for name, value in bind_seq}
    return where_sql, binds


def run_rate(inquiry_id: int, rating: int, comment: str) -> Dict[str, Any]:
    table = get_setting("DW_CONTRACT_TABLE", scope="namespace") or "Contract"
    date_col = get_setting("DW_DATE_COLUMN", scope="namespace") or "REQUEST_DATE"
    eq_alias_raw = get_setting("DW_EQ_ALIAS_COLUMNS", scope="namespace") or {}
    fts_columns_raw = get_setting("DW_FTS_COLUMNS", scope="namespace") or {}
    fts_engine = get_setting("DW_FTS_ENGINE", scope="namespace") or "like"
    validate_only = _get_bool_setting("VALIDATE_WITH_EXPLAIN_ONLY", default=False)
    intent = parse_rate_comment(comment or "")

    rt_syn = _request_type_synonyms()

    if "REQUEST_TYPE" in intent.get("eq", {}):
        vals = intent["eq"]["REQUEST_TYPE"]

        def _expand_one(value: str) -> List[str]:
            key = value.strip().lower()
            if key in rt_syn and isinstance(rt_syn[key], dict):
                equals = rt_syn[key].get("equals")
                if isinstance(equals, list) and equals:
                    return [str(v) for v in equals if v]
            return [value]

        expanded: List[str] = []
        for value in vals:
            expanded.extend(_expand_one(value))
        intent["eq"]["REQUEST_TYPE"] = expanded

    settings_map = {
        "DW_EQ_ALIAS_COLUMNS": eq_alias_raw,
        "DW_FTS_COLUMNS": fts_columns_raw,
        "DW_FTS_ENGINE": fts_engine,
    }

    where_sql, binds = build_where_sql(intent, settings_map)
    order_clause = intent.get("order_by") or f"{date_col} DESC"
    sql = f'SELECT * FROM "{table}"\nWHERE {where_sql}\nORDER BY {order_clause}'

    columns: List[str] = []
    rows: List[List[Any]] = []
    row_count = 0

    if not validate_only:
        app_engine = get_engine_for_default_datasource()
        logger.info(
            {
                "event": "rate.sql.exec",
                "inquiry_id": inquiry_id,
                "sql": sql,
                "binds": binds,
            }
        )
        columns, rows, row_count = execute_select(app_engine, sql, binds, max_rows=500)
        logger.info(
            {
                "event": "rate.sql.done",
                "inquiry_id": inquiry_id,
                "rows": row_count,
                "columns_count": len(columns),
            }
        )

    debug_intent = {
        "fts_groups": intent.get("fts_groups", []),
        "eq_filters": [
            {"col": col, "values": cleaned}
            for col, values in intent.get("eq", {}).items()
            for cleaned in [_clean_values(values)]
            if cleaned
        ],
        "neq_filters": [
            {"col": col, "values": cleaned}
            for col, values in intent.get("neq", {}).items()
            for cleaned in [_clean_values(values)]
            if cleaned
        ],
        "contains": list(intent.get("contains", {}).keys()),
        "not_contains": list(intent.get("not_contains", {}).keys()),
        "empty_any": intent.get("empty_any", []),
        "empty_all": intent.get("empty_all", []),
        "not_empty": intent.get("not_empty", []),
        "sort_by": order_clause,
    }

    debug_validation = {
        "ok": True,
        "bind_names": list(binds.keys()),
        "binds": binds,
        "errors": [],
        "row_count": row_count,
    }

    resp: Dict[str, Any] = {
        "ok": True,
        "retry": False,
        "inquiry_id": inquiry_id,
        "sql": sql,
        "binds": binds,
        "columns": columns,
        "rows": rows,
        "debug": {
            "final_sql": {"sql": sql, "size": len(sql)},
            "intent": debug_intent,
            "validation": debug_validation,
        },
    }
    return resp


__all__ = ["build_select_all", "run_rate"]
