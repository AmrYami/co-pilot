from __future__ import annotations

from typing import Dict, List, Optional, Tuple

from apps.dw.intent import NLIntent, NET_SQL


def _window_predicate(intent: NLIntent, overlap_strict: bool) -> Optional[str]:
    if not intent.has_time_window or not intent.explicit_dates:
        return None
    col = (intent.date_column or "OVERLAP").upper()
    if col == "REQUEST_DATE":
        return "REQUEST_DATE BETWEEN :date_start AND :date_end"
    if col == "START_DATE":
        return "START_DATE BETWEEN :date_start AND :date_end"
    if col == "END_DATE":
        return "END_DATE BETWEEN :date_start AND :date_end"
    if overlap_strict:
        return "(START_DATE <= :date_end AND END_DATE >= :date_start)"
    return "((START_DATE IS NULL OR START_DATE <= :date_end) AND (END_DATE IS NULL OR END_DATE >= :date_start))"


def _fts_predicate(columns: List[str], token_bind_names: List[str]) -> str:
    ors = []
    for column in columns:
        for bind_name in token_bind_names:
            ors.append(f"INSTR(UPPER({column}), :{bind_name}) > 0")
    return "(" + " OR ".join(ors) + ")"


def build_sql(
    intent: NLIntent,
    *,
    table: str = '"Contract"',
    overlap_strict: bool = True,
    fts_columns: Optional[List[str]] = None,
) -> Tuple[str, Dict[str, object]]:
    where_parts: List[str] = []
    binds: Dict[str, object] = {}

    window_sql = _window_predicate(intent, overlap_strict=overlap_strict)
    if window_sql:
        where_parts.append(window_sql)
        ds = intent.explicit_dates or {}
        if "start" in ds and "end" in ds:
            binds["date_start"] = ds["start"]
            binds["date_end"] = ds["end"]

    token_bind_names: List[str] = []
    if intent.full_text_search and intent.fts_tokens and fts_columns:
        for i, token in enumerate(intent.fts_tokens, start=1):
            bind_name = f"ft_{i}"
            token_bind_names.append(bind_name)
            binds[bind_name] = token
        where_parts.append(_fts_predicate(fts_columns, token_bind_names))

    where_sql = ""
    if where_parts:
        where_sql = "WHERE " + " AND ".join(where_parts)

    agg = intent.agg
    group_by = intent.group_by
    top_n = intent.top_n
    measure = intent.measure_sql or NET_SQL
    projection = intent.projection
    wants_all = intent.wants_all_columns if intent.wants_all_columns is not None else True

    if agg == "count":
        if group_by:
            select_cols = [f"{group_by} AS GROUP_KEY", "COUNT(*) AS CNT"]
        else:
            select_cols = ["COUNT(*) AS CNT"]
    elif group_by:
        select_cols = [f"{group_by} AS GROUP_KEY", f"SUM({measure}) AS MEASURE"]
    else:
        if projection:
            select_cols = projection
        elif wants_all:
            select_cols = ["*"]
        else:
            select_cols = [
                "CONTRACT_ID",
                "CONTRACT_OWNER",
                "REQUEST_DATE",
                "START_DATE",
                "END_DATE",
            ]

    sql_lines: List[str] = ["SELECT", "  " + ",\n  ".join(select_cols), f"FROM {table}"]
    if where_sql:
        sql_lines.append(where_sql)

    if agg and group_by:
        sql_lines.append(f"GROUP BY {group_by}")

    order_clause = ""
    if agg == "count" and group_by:
        order_clause = "ORDER BY CNT DESC"
    elif group_by:
        order_expr = intent.sort_by or "MEASURE"
        direction = "DESC" if intent.sort_desc is not False else "ASC"
        order_clause = f"ORDER BY {order_expr if intent.sort_by else 'MEASURE'} {direction}"
    elif agg == "count":
        order_clause = ""
    else:
        order_expr = intent.sort_by or (measure if top_n else "REQUEST_DATE")
        direction = "DESC" if intent.sort_desc is not False else "ASC"
        order_clause = f"ORDER BY {order_expr} {direction}"

    if order_clause:
        sql_lines.append(order_clause)

    if top_n:
        binds["top_n"] = top_n
        sql_lines.append("FETCH FIRST :top_n ROWS ONLY")

    sql = "\n".join(sql_lines)
    return sql, binds
