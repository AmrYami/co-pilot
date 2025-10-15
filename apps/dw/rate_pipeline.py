from __future__ import annotations

from typing import Any, Dict, Iterable, List, Tuple

from apps.dw.logger import log
from apps.dw.rate_intent import build_where_and_binds, parse_structured_comment
from apps.dw.sql_shared import dw_date_col, dw_table, exec_sql
from apps.dw.settings import get_setting


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
    intent = parse_structured_comment(comment or "")
    where_sql, binds = build_where_and_binds(table, intent)
    order_clause = intent.order_by or f"{date_column} DESC"
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
        "empty": list(intent.empty),
        "not_empty": list(intent.not_empty),
        "empty_any": [list(group) for group in intent.empty_any],
        "empty_all": [list(group) for group in intent.empty_all],
        "fts_groups": [list(group) for group in intent.fts_groups],
        "sort_by": order_clause,
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
