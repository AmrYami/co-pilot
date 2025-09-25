from __future__ import annotations

from typing import Dict, List, Tuple
import os


STRICT_OVERLAP = os.getenv("DW_STRICT_OVERLAP", "0").lower() in {"1", "true", "yes"}


def overlap_predicate() -> str:
    """Return the contract/activity window overlap predicate."""
    if STRICT_OVERLAP:
        return "(START_DATE <= :date_end AND END_DATE >= :date_start)"
    return (
        "((START_DATE IS NULL OR START_DATE <= :date_end) "
        "AND (END_DATE IS NULL OR END_DATE >= :date_start))"
    )


def window_predicate(date_column: str) -> str:
    col = (date_column or "").upper()
    if col == "REQUEST_DATE":
        return "REQUEST_DATE BETWEEN :date_start AND :date_end"
    if col == "START_DATE":
        return "START_DATE BETWEEN :date_start AND :date_end"
    if col == "END_DATE":
        return "END_DATE BETWEEN :date_start AND :date_end"
    return overlap_predicate()


def build_top_contracts_sql(measure_sql: str, date_semantics: str, select_all: bool) -> str:
    where = window_predicate(date_semantics)
    if select_all:
        select = "*"
    else:
        select = (
            "CONTRACT_ID, CONTRACT_OWNER, OWNER_DEPARTMENT, DEPARTMENT_OUL, "
            "ENTITY_NO, {measure} AS MEASURE, START_DATE, END_DATE"
        ).format(measure=measure_sql)
    return (
        f'SELECT {select} FROM "Contract"\n'
        f'WHERE {where}\n'
        f'ORDER BY {measure_sql} DESC\n'
        f'FETCH FIRST :top_n ROWS ONLY'
    )


def build_grouped_sql(
    group_by: str,
    agg: str,
    measure_sql: str,
    date_semantics: str,
    top_n: int | None,
) -> str:
    where = window_predicate(date_semantics)
    agg = (agg or "sum").lower()
    if agg == "count":
        agg_expr = "COUNT(*)"
        metric = "CNT"
    else:
        agg_expr = f"SUM({measure_sql})"
        metric = "MEASURE"
    limit = "\nFETCH FIRST :top_n ROWS ONLY" if top_n else ""
    return (
        f"SELECT NVL({group_by}, '(Unknown)') AS GROUP_KEY, {agg_expr} AS {metric}\n"
        f'FROM "Contract"\n'
        f"WHERE {where}\n"
        f"GROUP BY NVL({group_by}, '(Unknown)')\n"
        f"ORDER BY {metric} DESC{limit}"
    )


def build_count_expiring_soon(days: int) -> Tuple[str, Dict[str, int]]:
    sql = (
        'SELECT COUNT(*) AS CNT\n'
        'FROM "Contract"\n'
        "WHERE END_DATE BETWEEN TRUNC(SYSDATE) AND TRUNC(SYSDATE) + :days"
    )
    return sql, {"days": days}


def build_scan_all_sql(text_columns: List[str]) -> str:
    if not text_columns:
        raise ValueError("text_columns required for scan")
    ors = " OR ".join([f"UPPER({col}) LIKE UPPER(:scan_pat)" for col in text_columns])
    return (
        f'SELECT * FROM "Contract"\n'
        f"WHERE {ors}\n"
        f"ORDER BY REQUEST_DATE DESC\n"
        f"FETCH FIRST 200 ROWS ONLY"
    )

