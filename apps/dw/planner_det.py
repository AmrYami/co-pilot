from __future__ import annotations

from typing import Any, Dict, Tuple, Optional, TYPE_CHECKING

from apps.dw.tables import for_namespace
from apps.dw.tables.base import TableSpec

if TYPE_CHECKING:
    from core.settings import Settings


def _predicate(intent: Dict[str, Any], spec: TableSpec, settings: "Settings", binds: Dict[str, Any]) -> Optional[str]:
    explicit = intent.get("explicit_dates")
    if not isinstance(explicit, dict):
        explicit = None
    start = explicit.get("start") if explicit else None
    end = explicit.get("end") if explicit else None
    if not (start and end):
        return None

    binds["date_start"] = start
    binds["date_end"] = end

    mode = intent.get("date_column") or spec.default_date_mode
    strict = bool(settings.get_bool("DW_OVERLAP_STRICT", False))

    if intent.get("expire"):
        return spec.request_date_predicate(start_bind=":date_start", end_bind=":date_end").replace(
            spec.request_date_col, spec.end_date_col
        )
    if mode == "REQUEST_DATE":
        return spec.request_date_predicate()
    if mode == "OVERLAP":
        return spec.overlap_predicate(strict=strict)
    if isinstance(mode, str):
        return f"{mode} BETWEEN :date_start AND :date_end"
    return None


def _projection(intent: Dict[str, Any], spec: TableSpec) -> Tuple[str, Optional[str], Optional[str]]:
    group_by = intent.get("group_by")
    agg = (intent.get("agg") or "").lower() or None
    measure = intent.get("measure_sql") or spec.net_expr()
    wants_all = bool(intent.get("wants_all_columns", True))
    notes = intent.get("notes") if isinstance(intent.get("notes"), dict) else {}
    projection_list = notes.get("projection") if isinstance(notes, dict) else None

    if group_by:
        if agg == "count":
            measure_expr = "COUNT(*)"
        elif agg == "avg":
            measure_expr = f"AVG({measure})"
        elif agg == "sum":
            measure_expr = f"SUM({measure})"
        else:
            measure_expr = f"SUM({measure})"
        return f"{group_by} AS GROUP_KEY, {measure_expr} AS MEASURE", "MEASURE", agg

    if agg == "count":
        return "COUNT(*) AS MEASURE", "MEASURE", agg

    if isinstance(projection_list, list) and projection_list:
        projection_sql = ", ".join(projection_list)
    elif wants_all:
        projection_sql = "*"
    else:
        projection_sql = "CONTRACT_ID, CONTRACT_OWNER, REQUEST_DATE, START_DATE, END_DATE"
    return projection_sql, measure, agg


def build_sql(intent: Dict[str, Any], settings: "Settings") -> Tuple[str, Dict[str, Any], str]:
    spec = for_namespace(settings)
    binds: Dict[str, Any] = {}
    predicate = _predicate(intent, spec, settings, binds)
    projection, order_measure, _ = _projection(intent, spec)
    group_by = intent.get("group_by")
    top_n = intent.get("top_n")
    sort_by = intent.get("sort_by")
    sort_desc = bool(intent.get("sort_desc", True))

    lines = ["SELECT", f"  {projection}", f"FROM \"{spec.name}\""]
    if predicate:
        lines.append(f"WHERE {predicate}")

    if group_by:
        lines.append(f"GROUP BY {group_by}")

    order_clause = ""
    if sort_by:
        direction = "DESC" if sort_desc else "ASC"
        order_clause = f"ORDER BY {sort_by} {direction}"
    elif order_measure:
        order_clause = f"ORDER BY {order_measure} DESC"

    if order_clause:
        lines.append(order_clause)

    if top_n:
        binds["top_n"] = top_n
        lines.append("FETCH FIRST :top_n ROWS ONLY")

    sql = "\n".join(lines)

    explain_bits = []
    if intent.get("expire"):
        explain_bits.append("Filtering by END_DATE window (expiring).")
    elif predicate and intent.get("date_column") == "REQUEST_DATE":
        explain_bits.append("Filtering by REQUEST_DATE window.")
    elif predicate:
        explain_bits.append("Treating contracts as active by date overlap.")

    if group_by:
        explain_bits.append(f"Grouping by {group_by}.")

    gross_expr = spec.gross_expr()
    if intent.get("measure_sql") == gross_expr or "gross" in (intent.get("raw") or "").lower():
        explain_bits.append("Sorting by GROSS value.")
    else:
        explain_bits.append("Sorting by NET value.")

    if top_n:
        explain_bits.append(f"Limiting to Top {top_n}.")

    explain = " ".join(explain_bits)
    return sql, binds, explain
