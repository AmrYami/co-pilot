from __future__ import annotations

from typing import Any, Dict, Tuple


__all__ = ["build_sql"]


def build_sql(intent: Dict[str, Any], table: str = "Contract") -> Tuple[str, Dict[str, Any]]:
    intent = intent or {}
    binds: Dict[str, Any] = {}
    date_col = intent.get("date_column") or "REQUEST_DATE"
    where_parts: list[str] = []

    explicit = intent.get("explicit_dates") or {}
    start = explicit.get("start")
    end = explicit.get("end")
    if start and end:
        binds["date_start"] = start
        binds["date_end"] = end
        where_parts.append(f"{date_col} BETWEEN :date_start AND :date_end")

    group_by = intent.get("group_by")
    agg = intent.get("agg")

    select_clause: str
    group_clause = ""
    order_clause = ""

    table_literal = f'"{table}"' if not table.startswith('"') else table

    if group_by:
        if agg in (None, "sum", "gross"):
            measure = intent.get("measure_sql") or "NVL(CONTRACT_VALUE_NET_OF_VAT,0)"
            alias = "GROSS_VALUE" if "GROSS" in measure.upper() else "NET_VALUE"
            select_clause = f"SELECT {group_by} AS GROUP_KEY, SUM({measure}) AS {alias}"
            group_clause = f"GROUP BY {group_by}"
            order_by = intent.get("sort_by") or alias
            order_desc = intent.get("sort_desc", True)
            order_clause = f"ORDER BY {order_by} {'DESC' if order_desc else 'ASC'}"
        elif agg == "count":
            select_clause = f"SELECT {group_by} AS GROUP_KEY, COUNT(*) AS CNT"
            group_clause = f"GROUP BY {group_by}"
            order_clause = "ORDER BY CNT DESC"
        else:
            return "", {}
    else:
        if agg == "count":
            select_clause = "SELECT COUNT(*) AS CNT"
        else:
            if intent.get("wants_all_columns", True):
                select_clause = "SELECT *"
            else:
                select_clause = "SELECT *"
            order_target = intent.get("sort_by") or date_col
            order_desc = intent.get("sort_desc")
            if order_target:
                direction = "DESC" if order_desc else "ASC"
                order_clause = f"ORDER BY {order_target} {direction}"

    sql_parts = [select_clause, f"FROM {table_literal}"]
    if where_parts:
        sql_parts.append("WHERE " + " AND ".join(where_parts))
    if group_clause:
        sql_parts.append(group_clause)
    if order_clause:
        sql_parts.append(order_clause)

    top_n = intent.get("top_n")
    if top_n:
        binds["top_n"] = int(top_n)
        sql_parts.append("FETCH FIRST :top_n ROWS ONLY")

    return " ".join(sql_parts), binds
