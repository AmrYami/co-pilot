from __future__ import annotations

from typing import Any, Dict, Iterable, List, Tuple

from apps.dw.logger import log
from apps.dw.rate_intent import _normalized, build_where_and_binds, parse_structured_comment
from apps.dw.sql_shared import dw_date_col, dw_table, exec_sql, explicit_columns, eq_alias_columns
from apps.dw.settings import get_setting
from apps.dw.contracts.fts import extract_fts_terms


def build_select_all(table: str, order_by: str | None = None, desc: bool = True) -> str:
    base = f'SELECT * FROM "{table}"'
    if not order_by:
        return base
    direction = "DESC" if desc else "ASC"
    return f"{base}\nORDER BY {order_by} {direction}"


def _select_sql(table: str, where_sql: str, order_by: str) -> str:
    return f'SELECT * FROM "{table}"\nWHERE ({where_sql})\nORDER BY {order_by}'


def get_setting_json(key, *, scope=None, namespace=None, default=None):  # pragma: no cover - shim
    value = get_setting(key)
    if value is None:
        return default
    return value


def get_setting_value(key, *, scope=None, namespace=None, default=None):  # pragma: no cover - shim
    value = get_setting(key)
    if value is None:
        return default
    return value


def run_query(sql: str, binds: Dict[str, Any]) -> Tuple[List[str], List[List[Any]]]:
    return exec_sql(sql, binds)


def run_rate(inquiry_id: int, rating: int, comment: str) -> Dict[str, Any]:
    table = dw_table()
    date_column = dw_date_col()
    alias_map = eq_alias_columns() or {}
    allowed_cols = set(explicit_columns())
    for targets in alias_map.values():
        for col in targets:
            allowed_cols.add(str(col or "").strip().upper())

    try:
        intent = parse_structured_comment(comment or "", alias_map=alias_map, allowed_columns=allowed_cols)
    except ValueError as exc:
        return {
            "ok": False,
            "retry": False,
            "inquiry_id": inquiry_id,
            "error": str(exc),
        }
    # Deduplicate numeric predicates for stability
    if intent.numeric:
        dedup_numeric: List[Tuple[str, str, List[str]]] = []
        seen_numeric: set[Tuple[str, str, Tuple[str, ...]]] = set()
        for col, op, vals in intent.numeric:
            values_list = list(vals or [])
            key = (col.upper(), op.lower(), tuple(values_list))
            if key in seen_numeric:
                continue
            seen_numeric.add(key)
            dedup_numeric.append((col.upper(), op.lower(), values_list))
        intent.numeric = dedup_numeric

    eq_shape: Dict[str, Dict[str, Any]] = {}
    for col, vals in intent.eq_filters:
        normalized = _normalized(vals)
        eq_shape[col.upper()] = {
            "op": "in" if len(normalized) > 1 else "eq",
            "count": len(normalized),
        }

    numeric_shape: Dict[str, Dict[str, Any]] = {}
    for col, op, vals in intent.numeric:
        bucket = numeric_shape.setdefault(col.upper(), {"ops": set(), "count": 0})
        bucket["ops"].add(op.lower())
        bucket["count"] += 1
    numeric_shape = {col: {"ops": sorted(list(meta["ops"])), "count": meta["count"]} for col, meta in numeric_shape.items()}

    try:
        log.info(
            {
                "event": "rate.intent.built",
                "eq_cols": len(eq_shape),
                "numeric_cols": len(numeric_shape),
                "fts_groups": len(intent.fts_groups or []),
                "fts_tokens": sum(len(group) for group in (intent.fts_groups or [])),
            }
        )
    except Exception:
        pass

    where_sql, binds = build_where_and_binds(table, intent)

    group_by_cols = [col.strip().upper() for col in (intent.group_by or []) if str(col or "").strip()]
    aggregations = intent.aggregations or []
    gross_flag = bool(intent.gross) if intent.gross is not None else False

    sort_col = None
    sort_desc = True
    if intent.order_by:
        tokens = [tok for tok in intent.order_by.split() if tok]
        if tokens:
            sort_col = tokens[0].upper()
            if len(tokens) > 1 and tokens[1].upper() == "ASC":
                sort_desc = False
    order_clause = (
        f"{sort_col} {'DESC' if sort_desc else 'ASC'}" if sort_col else f"{date_column} DESC"
    )

    needs_aggregation = bool(group_by_cols or aggregations or gross_flag)
    if needs_aggregation:
        select_parts: List[str] = []
        if group_by_cols:
            select_parts.extend(group_by_cols)

        agg_parts: List[str] = []
        if aggregations:
            for agg in aggregations:
                func = str(agg.get("func") or "").upper()
                column = str(agg.get("column") or "").upper()
                distinct = bool(agg.get("distinct"))
                alias = agg.get("alias")
                if not func:
                    continue
                inner = "*"
                if column and column != "*":
                    inner = column
                if distinct and inner != "*":
                    inner = f"DISTINCT {inner}"
                expr = f"{func}({inner})"
                if alias:
                    expr += f" AS {str(alias).upper()}"
                agg_parts.append(expr)
        else:
            if gross_flag:
                agg_parts.append("SUM(CONTRACT_VALUE_NET_OF_VAT) AS TOTAL_AMOUNT")
            else:
                agg_parts.append("COUNT(*) AS TOTAL_COUNT")

        if agg_parts:
            select_parts.extend(agg_parts)

        if not select_parts:
            select_parts.append("*")

        select_clause = ", ".join(select_parts)
        sql = f'SELECT {select_clause}\nFROM "{table}"\nWHERE ({where_sql})'
        if group_by_cols:
            sql += "\nGROUP BY " + ", ".join(group_by_cols)
        if order_clause:
            sql += "\nORDER BY " + order_clause
    else:
        sql = _select_sql(table, where_sql, order_clause)

    validate_only = bool(get_setting("VALIDATE_WITH_EXPLAIN_ONLY"))

    columns: List[str] = []
    rows: List[List[Any]] = []

    log.info(
        {
            "event": "rate.sql.exec",
            "inquiry_id": inquiry_id,
            "sql": sql,
            "binds": binds,
        }
    )

    if not validate_only:
        try:
            result = run_query(sql, binds)
            if isinstance(result, tuple):
                columns, rows = result
            elif isinstance(result, Iterable):
                rows = list(result)
                columns = []
            else:
                columns, rows = [], []
            log.info(
                {
                    "event": "rate.sql.done",
                    "inquiry_id": inquiry_id,
                    "rows": len(rows),
                    "columns_count": len(columns),
                }
            )
        except Exception as exc:  # pragma: no cover - defensive logging
            log.exception("rate.sql.error", extra={"inquiry_id": inquiry_id, "error": str(exc)})

    debug_intent = {
        "eq_filters": [(col, values) for col, values in intent.eq_filters],
        "neq_filters": [(col, values) for col, values in intent.neq_filters],
        "contains": [(col, values) for col, values in intent.contains],
        "not_contains": [(col, values) for col, values in intent.not_contains],
        "numeric": [(col, op, values) for col, op, values in intent.numeric],
        "eq_shape": eq_shape,
        "numeric_shape": numeric_shape,
        "empty": list(intent.empty),
        "not_empty": list(intent.not_empty),
        "empty_any": [list(group) for group in intent.empty_any],
        "empty_all": [list(group) for group in intent.empty_all],
        "fts_groups": [list(group) for group in intent.fts_groups],
        "group_by": list(group_by_cols),
        "aggregations": [dict(entry) for entry in aggregations],
        "gross": intent.gross,
        "sort_by": sort_col or date_column,
        "sort_desc": sort_desc,
    }

    debug_validation = {
        "ok": True,
        "bind_names": list(binds.keys()),
        "binds": binds,
        "errors": [],
    }

    response: Dict[str, Any] = {
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

    return response


__all__ = ["build_select_all", "run_rate"]
