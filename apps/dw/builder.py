from __future__ import annotations
from __future__ import annotations

import re
from typing import Any, Dict, List, Tuple

from .intent import NLIntent
from .sql_builders import window_predicate
from .utils import env_flag

TABLE = '"Contract"'


def _gross_expr() -> str:
    return (
        "NVL(CONTRACT_VALUE_NET_OF_VAT,0) + "
        "CASE WHEN NVL(VAT,0) BETWEEN 0 AND 1 "
        "THEN NVL(CONTRACT_VALUE_NET_OF_VAT,0) * NVL(VAT,0) "
        "ELSE NVL(VAT,0) END"
    )


def _where_from_eq_filters(eq_filters: List[dict], binds: Dict[str, Any]) -> str:
    clauses: List[str] = []
    for idx, raw in enumerate(eq_filters or []):
        col = (raw.get("col") or raw.get("column") or "").strip()
        if not col:
            continue
        op = (raw.get("op") or ("like" if "pattern" in raw else "eq")).lower()
        val = (
            raw.get("val")
            if raw.get("val") is not None
            else raw.get("value")
            if raw.get("value") is not None
            else raw.get("pattern")
        )
        if val is None:
            continue
        ci = bool(raw.get("ci"))
        trim = bool(raw.get("trim"))
        bind = f"eq_{idx}"

        bind_val = val
        if trim and isinstance(bind_val, str):
            bind_val = bind_val.strip()
        if op == "like" and isinstance(bind_val, str) and "%" not in bind_val:
            bind_val = f"%{bind_val}%"

        binds[bind] = bind_val

        col_expr = col.upper()
        rhs_expr = f":{bind}"
        if trim:
            col_expr = f"TRIM({col_expr})"
            rhs_expr = f"TRIM({rhs_expr})"
        if ci:
            col_expr = f"UPPER({col_expr})"
            rhs_expr = f"UPPER({rhs_expr})"

        if op == "like":
            clauses.append(f"{col_expr} LIKE {rhs_expr}")
        else:
            clauses.append(f"{col_expr} = {rhs_expr}")

    return " AND ".join(clauses)


def build_sql(intent: NLIntent) -> Tuple[str, Dict[str, Any]]:
    binds: Dict[str, Any] = {}
    where_clauses = []
    order_clause = ""
    select_cols = "*"

    if intent.explicit_dates:
        binds["date_start"] = intent.explicit_dates["start"]
        binds["date_end"] = intent.explicit_dates["end"]
        if intent.expire:
            where_clauses.append("END_DATE BETWEEN :date_start AND :date_end")
        else:
            where_clauses.append(window_predicate(intent.date_column or "OVERLAP"))

    eq_clause = _where_from_eq_filters(getattr(intent, "eq_filters", []) or [], binds)
    if eq_clause:
        where_clauses.append(eq_clause)

    # Manual filters injected by planners (optional)
    manual_where = getattr(intent, "manual_where", None)
    if manual_where:
        where_clauses.append(f"({manual_where})")
    manual_binds = getattr(intent, "manual_binds", None)
    if isinstance(manual_binds, dict):
        binds.update(manual_binds)

    measure = intent.measure_sql or "NVL(CONTRACT_VALUE_NET_OF_VAT,0)"
    group_by = (intent.group_by or "").strip()
    sort_by = (intent.sort_by or "").strip()
    sort_desc = intent.sort_desc if intent.sort_desc is not None else True

    where_sql = " AND ".join(where_clauses)

    if intent.agg == "count" and not group_by:
        sql = f"SELECT COUNT(*) AS CNT FROM {TABLE}"
        if where_sql:
            sql += f"\nWHERE {where_sql}"
        return sql, binds

    if group_by:
        gb_cols = [c.strip() for c in group_by.split(",") if c.strip()]
        gb = ", ".join(gb_cols) if gb_cols else group_by
        wants_gross = bool(intent.gross) or sort_by.upper() == "TOTAL_GROSS"

        if wants_gross:
            gross = _gross_expr()
            sql = (
                f"SELECT {gb} AS GROUP_KEY,\n"
                f"       SUM({gross}) AS TOTAL_GROSS,\n"
                f"       COUNT(*) AS CNT\n"
                f"FROM {TABLE}"
            )
            if where_sql:
                sql += f"\nWHERE {where_sql}"
            sql += f"\nGROUP BY {gb}"
            sql += f"\nORDER BY TOTAL_GROSS {'DESC' if sort_desc else 'ASC'}"
            if intent.top_n:
                binds["top_n"] = intent.top_n
                sql += "\nFETCH FIRST :top_n ROWS ONLY"
            return sql, binds

        sql = (
            f"SELECT\n  {gb} AS GROUP_KEY,\n  SUM({measure}) AS MEASURE\nFROM {TABLE}"
        )
        if where_sql:
            sql += f"\nWHERE {where_sql}"
        sql += f"\nGROUP BY {gb}"
        sql += f"\nORDER BY MEASURE {'DESC' if sort_desc else 'ASC'}"
        if intent.top_n:
            binds["top_n"] = intent.top_n
            sql += "\nFETCH FIRST :top_n ROWS ONLY"
        return sql, binds

    wanted = (intent.notes or {}).get("projection")
    if wanted:
        select_cols = ", ".join(wanted)
        sql = f"SELECT {select_cols} FROM {TABLE}"
    elif env_flag("DW_SELECT_ALL_DEFAULT", True) or intent.wants_all_columns:
        sql = f"SELECT * FROM {TABLE}"
    else:
        sql = (
            "SELECT CONTRACT_ID, CONTRACT_OWNER, REQUEST_DATE, START_DATE, END_DATE, "
            "CONTRACT_VALUE_NET_OF_VAT, VAT FROM {table}".format(table=TABLE)
        )

    if where_sql:
        sql += f"\nWHERE {where_sql}"

    if sort_by:
        sql += f"\nORDER BY {sort_by} {'DESC' if sort_desc else 'ASC'}"
    elif getattr(intent, "user_requested_top_n", False):
        sql += f"\nORDER BY {measure} DESC"
    elif eq_clause:
        sql += "\nORDER BY REQUEST_DATE DESC"
    else:
        sql += f"\nORDER BY {measure} {'DESC' if sort_desc else 'ASC'}"

    if intent.user_requested_top_n and intent.top_n:
        binds["top_n"] = intent.top_n
        sql += "\nFETCH FIRST :top_n ROWS ONLY"

    return sql, binds
