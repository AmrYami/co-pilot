"""SQL planner for DocuWare Contract table based on DWIntent."""

from __future__ import annotations

from datetime import date
from typing import Dict, Optional, Tuple

from .intent import DWIntent

DIMENSIONS_ALLOWED = {"OWNER_DEPARTMENT", "DEPARTMENT_OUL", "ENTITY_NO", "ENTITY"}


def _overlap_clause() -> str:
    return (
        "(START_DATE IS NOT NULL AND END_DATE IS NOT NULL "
        "AND START_DATE <= :date_end AND END_DATE >= :date_start)"
    )


def _build_window(intent: DWIntent, binds: Dict[str, object]) -> Tuple[Optional[str], Optional[str]]:
    """Return WHERE clause for window and the window kind label."""

    has_start = "date_start" in binds and binds["date_start"] is not None
    has_end = "date_end" in binds and binds["date_end"] is not None
    if not (has_start and has_end):
        return None, None

    if intent.date_column == "REQUEST_DATE":
        return "REQUEST_DATE BETWEEN :date_start AND :date_end", "REQUEST"
    if intent.date_column == "END_ONLY":
        return "END_DATE BETWEEN :date_start AND :date_end", "END_ONLY"
    return _overlap_clause(), "OVERLAP"


def _apply_sort_asc_if_bottom(intent: DWIntent, default_desc: bool) -> bool:
    """Return final sort_desc considering 'bottom/lowest' signals."""

    if intent.sort_desc is not None:
        return bool(intent.sort_desc)
    if intent.is_bottom:
        return False
    return default_desc


def build_owner_vs_oul_mismatch_sql() -> str:
    """Rows where OWNER_DEPARTMENT and DEPARTMENT_OUL differ (lead = OUL)."""

    return (
        'SELECT OWNER_DEPARTMENT, DEPARTMENT_OUL, COUNT(*) AS CNT
'
        'FROM "Contract"
'
        "WHERE DEPARTMENT_OUL IS NOT NULL
"
        "  AND NVL(TRIM(OWNER_DEPARTMENT),'(None)') <> NVL(TRIM(DEPARTMENT_OUL),'(None)')
"
        "GROUP BY OWNER_DEPARTMENT, DEPARTMENT_OUL
"
        "ORDER BY CNT DESC"
    )


def _apply_intent_binds(intent: DWIntent, binds: Dict[str, object]) -> None:
    if intent.explicit_dates:
        start = intent.explicit_dates.get("start")
        end = intent.explicit_dates.get("end")
        if start and "date_start" not in binds:
            binds["date_start"] = start
        if end and "date_end" not in binds:
            binds["date_end"] = end

    if intent.notes.get("ytd"):
        if "date_start" not in binds or "date_end" not in binds:
            today = date.today()
            binds.setdefault("date_start", date(today.year, 1, 1))
            binds.setdefault("date_end", today)


def build_sql(intent: DWIntent) -> Tuple[str, Dict[str, object], Dict[str, object]]:
    """Build final SQL + binds + meta for the Contract table based on resolved intent."""

    binds: Dict[str, object] = {}
    meta: Dict[str, object] = {}

    measure = intent.measure_sql or "NVL(CONTRACT_VALUE_NET_OF_VAT,0)"

    q_lower = (intent.question or "").lower()
    if intent.notes.get("owner_vs_oul") or ("vs" in q_lower and "department_oul" in q_lower):
        sql = build_owner_vs_oul_mismatch_sql()
        meta.update({"explain": "Owner vs OUL mismatch rows (non-equal)."})
        return sql, binds, meta

    _apply_intent_binds(intent, binds)

    where_sql, window_kind = _build_window(intent, binds)
    if window_kind:
        meta["window_kind"] = window_kind

    if intent.group_by is None:
        sort_desc = _apply_sort_asc_if_bottom(intent, default_desc=True)
        order_sql = f"ORDER BY {measure} {'DESC' if sort_desc else 'ASC'}"

        top_sql = ""
        if intent.top_n:
            binds["top_n"] = intent.top_n
            top_sql = "FETCH FIRST :top_n ROWS ONLY"

        where_clause = f"WHERE {where_sql}
" if where_sql else ""
        sql = (
            'SELECT * FROM "Contract"
'
            f"{where_clause}"
            f"{order_sql}
"
            f"{top_sql}"
        ).strip()

        meta.update({
            "explain": (
                f"{'Top' if sort_desc else 'Bottom'} {intent.top_n or ''} by "
                f"{'GROSS' if measure != 'NVL(CONTRACT_VALUE_NET_OF_VAT,0)' else 'NET'}"
            ).strip(),
            "binds": {k: v for k, v in binds.items() if k == "top_n"},
        })
        return sql, binds, meta

    group_col = intent.group_by
    if group_col not in DIMENSIONS_ALLOWED:
        group_col = "OWNER_DEPARTMENT"

    agg = (intent.agg or ("SUM" if measure != "COUNT(*)" else "COUNT")).upper()
    if agg not in {"SUM", "AVG", "COUNT", "MEDIAN"}:
        agg = "SUM"

    sort_desc = _apply_sort_asc_if_bottom(intent, default_desc=True)
    order_sql = f"ORDER BY MEASURE {'DESC' if sort_desc else 'ASC'}"

    top_sql = ""
    if intent.top_n:
        binds["top_n"] = intent.top_n
        top_sql = "FETCH FIRST :top_n ROWS ONLY"

    if agg == "COUNT":
        select_measure = "COUNT(*)"
    else:
        select_measure = f"{agg}({measure})"

    select_sql = (
        "SELECT
"
        f"  {group_col} AS GROUP_KEY,
"
        f"  {select_measure} AS MEASURE
"
    )

    where_clause = f"WHERE {where_sql}
" if where_sql else ""
    sql = (
        f'{select_sql}FROM "Contract"
'
        f"{where_clause}"
        f"GROUP BY {group_col}
"
        f"{order_sql}
"
        f"{top_sql}"
    ).strip()

    meta.update({
        "group_by": group_col,
        "agg": agg.lower(),
        "gross": measure != "NVL(CONTRACT_VALUE_NET_OF_VAT,0)",
        "explain": f"{agg.title()} per {group_col} using {window_kind or 'ALL_TIME'} window.",
        "binds": {k: v for k, v in binds.items() if k == "top_n"},
    })
    return sql, binds, meta
