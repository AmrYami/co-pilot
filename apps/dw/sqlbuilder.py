from __future__ import annotations

from typing import Any, Dict, Optional

from core.nlu.schema import NLIntent


def _intent_dates(intent: NLIntent) -> tuple[Optional[str], Optional[str]]:
    window = getattr(intent, "explicit_dates", None)
    if window is None:
        return None, None
    start = getattr(window, "start", None)
    end = getattr(window, "end", None)
    if start and end:
        return str(start), str(end)
    return None, None


def _window_clause(intent: NLIntent, alias: str = "") -> tuple[str, Dict[str, Any]]:
    start, end = _intent_dates(intent)
    if not start or not end:
        return "", {}
    column = intent.date_column or "REQUEST_DATE"
    col_expr = f"{alias}{column}"
    return f"WHERE {col_expr} BETWEEN :date_start AND :date_end", {
        "date_start": start,
        "date_end": end,
    }


def build_dw_sql(
    intent: NLIntent,
    table: str = '"Contract"',
    select_all_default: bool = True,
    auto_detail: bool = True,
) -> Optional[dict]:
    """Return a deterministic SQL payload or None when insufficient intent."""

    has_window = bool(_intent_dates(intent)[0] and _intent_dates(intent)[1])
    wants_topn = bool(intent.top_n)
    grouped = bool(intent.group_by)
    counting = intent.agg == "count"

    if not (has_window or wants_topn or grouped or counting):
        return None

    binds: Dict[str, Any] = {}
    where, window_binds = _window_clause(intent)
    binds.update(window_binds)

    if counting and not grouped:
        sql = f"SELECT COUNT(*) AS CNT FROM {table} {where}".strip()
        return {"sql": sql, "binds": binds, "detail": False}

    if grouped:
        dim = intent.group_by
        measure = intent.measure_sql or "NVL(CONTRACT_VALUE_NET_OF_VAT,0)"
        agg_sql = f"SUM({measure})"
        summary_lines = [
            f"SELECT {dim} AS GROUP_KEY, {agg_sql} AS MEASURE",
            f"FROM {table}",
        ]
        if where:
            summary_lines.append(where)
        summary_lines.append(f"GROUP BY {dim}")
        summary_lines.append("ORDER BY MEASURE DESC")
        if intent.top_n:
            summary_lines.append("FETCH FIRST :top_n ROWS ONLY")
            binds["top_n"] = intent.top_n
        summary_sql = "\n".join(summary_lines)
        result: Dict[str, Any] = {"sql": summary_sql, "binds": dict(binds), "detail": False}

        if auto_detail and intent.top_n:
            detail_lines = [
                "WITH top_dim AS (",
                f"  SELECT {dim} AS GROUP_KEY, {agg_sql} AS MEASURE",
                f"  FROM {table}",
            ]
            if where:
                detail_lines.append(f"  {where}")
            detail_lines.extend(
                [
                    f"  GROUP BY {dim}",
                    f"  ORDER BY MEASURE DESC",
                    f"  FETCH FIRST :top_n ROWS ONLY",
                    ")",
                    f"SELECT c.*",
                    f"FROM {table} c",
                    f"JOIN top_dim t ON c.{dim} = t.GROUP_KEY",
                ]
            )
            if where:
                detail_where = where.replace("WHERE ", "WHERE c.", 1)
                detail_lines.append(detail_where)
            detail_lines.append(f"ORDER BY t.MEASURE DESC, c.{intent.date_column} DESC")
            result["detail_sql"] = "\n".join(detail_lines)
            result["detail"] = True
        return result

    projection = "*" if select_all_default else (
        f"CONTRACT_ID, CONTRACT_OWNER, {intent.date_column} AS WINDOW_DATE, "
        f"{intent.measure_sql or 'NVL(CONTRACT_VALUE_NET_OF_VAT,0)'} AS VALUE"
    )
    lines = [
        f"SELECT {projection}",
        f"FROM {table}",
    ]
    if where:
        lines.append(where)
    if intent.top_n:
        order_by = intent.sort_by or intent.date_column or "REQUEST_DATE"
        direction = "DESC" if intent.sort_desc else "ASC"
        lines.append(f"ORDER BY {order_by} {direction}")
        lines.append("FETCH FIRST :top_n ROWS ONLY")
        binds["top_n"] = intent.top_n
    sql = "\n".join(lines)
    return {"sql": sql, "binds": binds, "detail": False}
