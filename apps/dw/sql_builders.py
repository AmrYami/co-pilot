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


# --- Rate overrides helpers -------------------------------------------------

GROSS_EXPR = (
    "NVL(CONTRACT_VALUE_NET_OF_VAT,0) + CASE "
    "WHEN NVL(VAT,0) BETWEEN 0 AND 1 "
    "THEN NVL(CONTRACT_VALUE_NET_OF_VAT,0) * NVL(VAT,0) "
    "ELSE NVL(VAT,0) END"
)


def _eq_condition(col: str, bind: str, *, ci: bool, trim: bool) -> str:
    lhs = col
    rhs = f":{bind}"
    if trim:
        lhs = f"TRIM({lhs})"
        rhs = f"TRIM({rhs})"
    if ci:
        lhs = f"UPPER({lhs})"
        rhs = f"UPPER({rhs})"
    return f"{lhs} = {rhs}"


def build_rate_fts_where(
    tokens: List[str], columns: List[str], op: str, binds: Dict[str, str]
) -> Tuple[str, Dict[str, str]]:
    if not tokens or not columns:
        return "", binds
    token_clauses: List[str] = []
    for index, token in enumerate(tokens):
        bind_name = f"fts_{index}"
        binds[bind_name] = f"%{token}%"
        column_matches = [f"UPPER(NVL({col},'')) LIKE UPPER(:{bind_name})" for col in columns]
        token_clauses.append("(" + " OR ".join(column_matches) + ")")
    glue = " AND " if (op or "OR").upper() == "AND" else " OR "
    return "(" + glue.join(token_clauses) + ")", binds


def build_rate_eq_where(
    eq_filters: List[Dict[str, str]], allowed_cols: List[str], binds: Dict[str, str]
) -> Tuple[str, Dict[str, str]]:
    if not eq_filters:
        return "", binds
    allowed = {col.upper() for col in allowed_cols or []}
    clauses: List[str] = []
    bind_index = 0
    for filt in eq_filters:
        col = str(filt.get("col") or "").upper().strip()
        if allowed and col not in allowed:
            continue
        bind_name = f"eq_{bind_index}"
        binds[bind_name] = filt.get("val") or ""
        clauses.append(
            _eq_condition(
                col,
                bind_name,
                ci=bool(filt.get("ci")),
                trim=bool(filt.get("trim")),
            )
        )
        bind_index += 1
    if not clauses:
        return "", binds
    return "(" + " AND ".join(clauses) + ")", binds


def _merge_where_clauses(lhs: str, rhs: str) -> str:
    if lhs and rhs:
        return f"{lhs} AND {rhs}"
    return lhs or rhs


def _order_clause(sort_by: str | None, sort_desc: bool | None) -> str:
    if not sort_by:
        return "ORDER BY REQUEST_DATE DESC"
    direction = "DESC" if sort_desc is not False else "ASC"
    return f"ORDER BY {sort_by} {direction}"


def select_all_sql(
    fts_tokens: List[str],
    fts_cols: List[str],
    fts_operator: str,
    eq_filters: List[Dict[str, str]],
    allowed_eq_cols: List[str],
    sort_by: str | None,
    sort_desc: bool | None,
) -> Tuple[str, Dict[str, str]]:
    binds: Dict[str, str] = {}
    fts_where, binds = build_rate_fts_where(fts_tokens, fts_cols, fts_operator, binds)
    eq_where, binds = build_rate_eq_where(eq_filters, allowed_eq_cols, binds)
    where_sql = _merge_where_clauses(fts_where, eq_where)
    sql = 'SELECT * FROM "Contract"'
    if where_sql:
        sql += f"\nWHERE {where_sql}"
    sql += "\n" + _order_clause(sort_by, sort_desc)
    return sql, binds


def group_by_sql(
    group_by: str,
    gross: bool,
    fts_tokens: List[str],
    fts_cols: List[str],
    fts_operator: str,
    eq_filters: List[Dict[str, str]],
    allowed_eq_cols: List[str],
    sort_by: str | None,
    sort_desc: bool | None,
) -> Tuple[str, Dict[str, str]]:
    binds: Dict[str, str] = {}
    fts_where, binds = build_rate_fts_where(fts_tokens, fts_cols, fts_operator, binds)
    eq_where, binds = build_rate_eq_where(eq_filters, allowed_eq_cols, binds)
    where_sql = _merge_where_clauses(fts_where, eq_where)

    select_expr = f"{group_by} AS GROUP_KEY"
    order_metric = "CNT"
    if gross:
        select_expr += f",\n       SUM({GROSS_EXPR}) AS TOTAL_GROSS"
        select_expr += ",\n       COUNT(*) AS CNT"
        order_metric = "TOTAL_GROSS"
    else:
        select_expr += ",\n       COUNT(*) AS CNT"

    sql = f'SELECT {select_expr}\nFROM "Contract"'
    if where_sql:
        sql += f"\nWHERE {where_sql}"
    sql += f"\nGROUP BY {group_by}"

    if not sort_by:
        sort_by = order_metric
        sort_desc = True
    sql += "\n" + _order_clause(sort_by, sort_desc)
    return sql, binds

