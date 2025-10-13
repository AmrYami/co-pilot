from __future__ import annotations

from typing import Any, Dict, List

from apps.dw.rate_dbexec import exec_sql_with_columns
from apps.dw.rate_parser import parse_rate_comment
from apps.dw.rate_sql import build_where

try:  # pragma: no cover - optional dependency during tests
    from apps.dw.settings import get_setting
except Exception:  # pragma: no cover - fallback when settings helper unavailable
    def get_setting(*_args, **kwargs):  # type: ignore[return-type]
        return kwargs.get("default")


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


def run_rate(inquiry_id: int, rating: int, comment: str) -> Dict[str, Any]:
    table = get_setting("DW_CONTRACT_TABLE", scope="namespace") or "Contract"
    date_col = get_setting("DW_DATE_COLUMN", scope="namespace") or "REQUEST_DATE"
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

    where_sql, binds = build_where(intent)
    where_clause = where_sql or "WHERE 1=1"
    order_by = intent.get("order_by") or f"{date_col} DESC"
    sql = f'SELECT * FROM "{table}"\n{where_clause}\nORDER BY {order_by}'

    columns, rows = exec_sql_with_columns(sql, binds, table)

    debug_intent = {
        "fts_groups": intent.get("fts_groups", []),
        "eq_filters": [
            {"col": col, "values": values} for col, values in intent.get("eq", {}).items()
        ],
        "neq_filters": [
            {"col": col, "values": values} for col, values in intent.get("neq", {}).items()
        ],
        "contains": list(intent.get("contains", {}).keys()),
        "not_contains": list(intent.get("not_contains", {}).keys()),
        "empty": intent.get("empty", []),
        "empty_any": intent.get("empty_any", []),
        "empty_all": intent.get("empty_all", []),
        "sort_by": order_by,
    }

    debug_validation = {
        "ok": True,
        "bind_names": list(binds.keys()),
        "binds": binds,
        "errors": [],
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
