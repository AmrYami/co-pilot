from __future__ import annotations
from __future__ import annotations

from typing import Dict, Tuple

from .intent import NLIntent
from .sql_builders import window_predicate
from .utils import env_flag

TABLE = '"Contract"'


def build_sql(intent: NLIntent) -> Tuple[str, Dict[str, str]]:
    binds: Dict[str, str] = {}
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

    is_agg = False
    if intent.agg == "count":
        is_agg = True
        sql = f"SELECT COUNT(*) AS CNT FROM {TABLE}"
        if where_clauses:
            sql += "\nWHERE " + " AND ".join(where_clauses)
        return sql, binds

    measure = intent.measure_sql or "NVL(CONTRACT_VALUE_NET_OF_VAT,0)"
    if intent.group_by:
        is_agg = True
        select_cols = f"{intent.group_by} AS GROUP_KEY, SUM({measure}) AS MEASURE"
        order_clause = "ORDER BY MEASURE DESC"
    elif intent.user_requested_top_n:
        order_clause = f"ORDER BY {measure} DESC"

    if is_agg:
        sql = f"SELECT\n  {select_cols}\nFROM {TABLE}"
    else:
        wanted = (intent.notes or {}).get("projection")
        if wanted and not is_agg:
            select_cols = ", ".join(wanted)
            sql = f"SELECT {select_cols} FROM {TABLE}"
        elif env_flag("DW_SELECT_ALL_DEFAULT", True) or intent.wants_all_columns:
            sql = f"SELECT * FROM {TABLE}"
        else:
            sql = (
                "SELECT CONTRACT_ID, CONTRACT_OWNER, REQUEST_DATE, START_DATE, END_DATE, "
                "CONTRACT_VALUE_NET_OF_VAT, VAT FROM {table}".format(table=TABLE)
            )

    if where_clauses:
        sql += "\nWHERE " + " AND ".join(where_clauses)

    if is_agg and " AS GROUP_KEY" in select_cols:
        group_col = select_cols.split(" AS GROUP_KEY")[0].split(",")[0].strip()
        sql += "\nGROUP BY " + group_col

    if order_clause:
        sql += "\n" + order_clause

    if intent.user_requested_top_n and intent.top_n:
        sql += "\nFETCH FIRST :top_n ROWS ONLY"
        binds["top_n"] = intent.top_n

    return sql, binds
