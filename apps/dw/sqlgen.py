from __future__ import annotations
import os
from typing import Tuple, Dict, Any, List, Optional
from .intent import DWIntent


def _env_flag(name: str, default: int = 0) -> int:
    v = os.getenv(name)
    if v is None:
        return default
    return 1 if str(v).lower() in ("1", "true", "yes", "y") else 0


def _contract_table() -> str:
    return os.getenv("DW_CONTRACT_TABLE", "Contract")


def _strict_overlap() -> bool:
    return bool(_env_flag("DW_OVERLAP_REQUIRE_BOTH_DATES", 1))


def _overlap_predicate(strict: Optional[bool] = None) -> str:
    if strict is None:
        strict = _strict_overlap()
    if strict:
        return "(START_DATE <= :date_end AND END_DATE >= :date_start)"
    return "((START_DATE IS NULL OR START_DATE <= :date_end) AND (END_DATE IS NULL OR END_DATE >= :date_start))"


def _date_filter(intent: DWIntent, *, strict_overlap: Optional[bool] = None) -> Tuple[str, Dict[str, Any]]:
    if not intent.has_time_window and not intent.explicit_dates:
        return ("", {})
    binds: Dict[str, Any] = {}
    if intent.date_column == "REQUEST_DATE":
        where = "REQUEST_DATE BETWEEN :date_start AND :date_end"
        binds["date_start"] = intent.explicit_dates["start"]
        binds["date_end"] = intent.explicit_dates["end"]
        return (where, binds)
    where = _overlap_predicate(strict_overlap)
    binds["date_start"] = intent.explicit_dates["start"]
    binds["date_end"] = intent.explicit_dates["end"]
    return (where, binds)


def _select_star_or_projection(intent: DWIntent) -> str:
    if intent.group_by or intent.agg:
        if intent.agg == "count" and intent.group_by:
            return f"{intent.group_by} AS GROUP_KEY, COUNT(*) AS CNT"
        if intent.group_by:
            m = intent.measure_sql or "NVL(CONTRACT_VALUE_NET_OF_VAT,0)"
            return f"{intent.group_by} AS GROUP_KEY, SUM({m}) AS MEASURE"
        if intent.agg == "count":
            return "COUNT(*) AS CNT"
    return "*"


def build_sql(intent: DWIntent, strict_overlap: Optional[bool] = None) -> Tuple[str, Dict[str, Any]]:
    table = _contract_table()
    select_clause = _select_star_or_projection(intent)
    where_parts: List[str] = []
    binds: Dict[str, Any] = {}

    w_sql, w_binds = _date_filter(intent, strict_overlap=strict_overlap)
    if w_sql:
        where_parts.append(w_sql)
    binds.update(w_binds)

    sql = [f"SELECT {select_clause}", f'FROM "{table}"']
    if where_parts:
        sql.append("WHERE " + " AND ".join(where_parts))

    if intent.group_by:
        sql.append(f"GROUP BY {intent.group_by}")
    if intent.sort_by:
        sql.append(f"ORDER BY {intent.sort_by} {'DESC' if intent.sort_desc else 'ASC'}")
    if intent.top_n:
        binds["top_n"] = intent.top_n
        sql.append("FETCH FIRST :top_n ROWS ONLY")

    return ("\n".join(sql), binds)
