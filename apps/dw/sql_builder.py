from __future__ import annotations

from typing import Dict, List, Optional, Tuple

from apps.dw.intent import NLIntent, GROSS_SQL, NET_SQL


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

    if intent.group_by:
        measure = intent.measure_sql or NET_SQL
        sql = (
            f"SELECT {intent.group_by} AS GROUP_KEY, SUM({measure}) AS MEASURE\n"
            f"FROM {table}\n"
            f"{where_sql}\n"
            f"GROUP BY {intent.group_by}\n"
            f"ORDER BY MEASURE DESC"
        )
        if intent.top_n:
            binds["top_n"] = intent.top_n
            sql += "\nFETCH FIRST :top_n ROWS ONLY"
        return sql, binds

    if intent.agg == "count" and not intent.group_by:
        sql = f"SELECT COUNT(*) AS CNT FROM {table}\n{where_sql}"
        return sql, binds

    projection = "*" if intent.wants_all_columns else "CONTRACT_ID, CONTRACT_OWNER, REQUEST_DATE, START_DATE, END_DATE"
    order_by = intent.sort_by or "REQUEST_DATE"
    sql = (
        f"SELECT {projection} FROM {table}\n"
        f"{where_sql}\n"
        f"ORDER BY {order_by} DESC"
    )
    if intent.top_n:
        binds["top_n"] = intent.top_n
        sql += "\nFETCH FIRST :top_n ROWS ONLY"
    return sql, binds
