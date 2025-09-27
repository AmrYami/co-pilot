from __future__ import annotations
from typing import Dict, Any, Tuple, List
from .types import NLIntent
from .sql_fragments import (
    expr_net, expr_gross, select_all, select_basic_contract,
    window_predicate, order_clause, limit_clause, union_stakeholders
)


def _projection_for_intent(it: NLIntent) -> str:
    # All columns unless question explicitly listed columns (not handled here) or it’s a grouped query
    if it.group_by and it.agg:
        # grouped selection
        if it.agg == "count":
            return f"{it.group_by} AS GROUP_KEY, COUNT(*) AS CNT"
        if it.agg == "avg":
            return f"{it.group_by} AS GROUP_KEY, AVG({it.measure_sql or expr_gross()}) AS MEASURE"
        # default sum
        return f"{it.group_by} AS GROUP_KEY, SUM({it.measure_sql or expr_net()}) AS MEASURE"
    if it.group_by and not it.agg:
        # “by X” but no measure → default SUM(net)
        return f"{it.group_by} AS GROUP_KEY, SUM({it.measure_sql or expr_net()}) AS MEASURE"
    # detail rows
    if it.wants_all_columns:
        return select_all()
    return select_basic_contract()


def _where_for_intent(it: NLIntent) -> str:
    if it.date_column:
        return window_predicate(it.date_column)
    # default (overlap) if the question has time window
    if it.has_time_window:
        return window_predicate("OVERLAP")
    return "1=1"


def _order_for_intent(it: NLIntent) -> str | None:
    # Grouped count → order by CNT desc
    if it.group_by and it.agg == "count":
        return "ORDER BY CNT DESC"
    # Grouped numeric measure
    if it.group_by and it.agg in ("sum","avg"):
        return "ORDER BY MEASURE DESC"
    # Detail “Top N … by …”
    if it.sort_by:
        return order_clause(it.sort_by, it.sort_desc if it.sort_desc is not None else True)
    return None


def _limit_for_intent(it: NLIntent) -> str | None:
    return limit_clause() if it.top_n else None


def _apply_fts(base_where: str, fts_cols: List[str] | None, tokens: List[str] | None) -> Tuple[str, Dict[str, Any]]:
    if not fts_cols or not tokens:
        return base_where, {}
    # Build (col1 LIKE :kw1 OR col2 LIKE :kw1 OR ...) AND (…kw2…)
    # safer: AND join groups, within each token we OR columns
    bind_params: Dict[str, Any] = {}
    clauses = [base_where] if base_where and base_where != "1=1" else []
    for i, tok in enumerate(tokens, start=1):
        ors = []
        b = f"kw{i}"
        bind_params[b] = f"%{tok}%"
        for c in fts_cols:
            ors.append(f"{c} LIKE :{b}")
        clauses.append("(" + " OR ".join(ors) + ")")
    return " AND ".join(clauses) if clauses else "1=1", bind_params


def build_sql_for_intent(it: NLIntent, table_name: str = "Contract",
                         fts_cols: List[str] | None = None) -> Tuple[str, Dict[str, Any]]:
    """
    Returns (sql, extra_binds). Binds :date_start, :date_end, :top_n are handled by caller.
    """
    # Stakeholder union special
    if it.group_by == "STAKEHOLDER_UNION":
        measure = it.measure_sql or expr_gross()
        where_pred = _where_for_intent(it)
        union_sql = union_stakeholders(8, measure, where_pred)
        sql = (
            "SELECT STAKEHOLDER AS GROUP_KEY, SUM(MEASURE) AS MEASURE\n"
            "FROM (\n" + union_sql + "\n)\n"
            "GROUP BY STAKEHOLDER\n"
            "ORDER BY MEASURE DESC"
        )
        if it.top_n:
            sql = sql + "\n" + "FETCH FIRST :top_n ROWS ONLY"
        return sql, {}

    select_clause = _projection_for_intent(it)
    where_clause = _where_for_intent(it)
    # Full-text search
    extra_binds: Dict[str, Any] = {}
    if it.full_text_search and it.fts_tokens:
        where_clause, fts_binds = _apply_fts(where_clause, fts_cols, it.fts_tokens)
        extra_binds.update(fts_binds)

    sql = f"SELECT {select_clause} FROM \"{table_name}\""
    if where_clause and where_clause != "1=1":
        sql += f"\nWHERE {where_clause}"

    order_sql = _order_for_intent(it)
    if order_sql:
        sql += f"\n{order_sql}"

    limit_sql = _limit_for_intent(it)
    if limit_sql:
        sql += f"\n{limit_sql}"

    return sql, extra_binds
