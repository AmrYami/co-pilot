from __future__ import annotations

from typing import Dict, Tuple

from .models import NLIntent
from .table_profiles import CONTRACT_TABLE, STAKEHOLDER_COLS, gross_sql, net_sql
from ..tables.contract import sql_total_gross_by_owner_department


def _window_clause(intent: NLIntent, strict_overlap: bool = True) -> Tuple[str, Dict[str, str]]:
    if intent.explicit_dates and intent.date_column and intent.date_column != "OVERLAP":
        column = intent.date_column
        return (
            f"{column} BETWEEN :date_start AND :date_end",
            {
                "date_start": intent.explicit_dates["start"],
                "date_end": intent.explicit_dates["end"],
            },
        )
    if intent.explicit_dates and intent.date_column == "OVERLAP":
        where = "(START_DATE <= :date_end AND END_DATE >= :date_start)"
        if strict_overlap:
            where = "(START_DATE IS NOT NULL AND END_DATE IS NOT NULL AND " + where[1:]
        return (
            where,
            {
                "date_start": intent.explicit_dates["start"],
                "date_end": intent.explicit_dates["end"],
            },
        )
    return "", {}


def _maybe_fetch_top(intent: NLIntent) -> str:
    if intent.top_n and intent.top_n > 0:
        return "FETCH FIRST :top_n ROWS ONLY"
    return ""


def _bind_top(intent: NLIntent, binds: Dict[str, object]) -> None:
    if intent.top_n and intent.top_n > 0:
        binds["top_n"] = intent.top_n


def _select_star(intent: NLIntent) -> str:
    if intent.wants_all_columns and not intent.group_by and not intent.agg:
        return "*"
    return "*"


def build_sql(
    intent: NLIntent,
    *,
    strict_overlap: bool = True,
    stakeholders_union: bool = False,
) -> Tuple[str, Dict[str, object]]:
    table = CONTRACT_TABLE
    binds: Dict[str, object] = {}

    if intent.filters.get("MISSING_ID") == "1":
        sql = (
            f'SELECT * FROM "{table}" '
            "WHERE (CONTRACT_ID IS NULL OR TRIM(CONTRACT_ID)='') "
            "ORDER BY REQUEST_DATE DESC"
        )
        return sql, binds

    if intent.filters.get("VAT_ZERO_AND_VALUE_POS") == "1":
        sql = (
            f'SELECT * FROM "{table}" '
            "WHERE NVL(VAT,0)=0 AND NVL(CONTRACT_VALUE_NET_OF_VAT,0)>0 "
            f"ORDER BY {net_sql()} DESC"
        )
        return sql, binds

    if intent.filters.get("MONTHLY_TREND") == "1":
        where, win_binds = _window_clause(intent, strict_overlap=False)
        clause = f"WHERE {where}" if where else ""
        sql = (
            f"SELECT TRUNC(REQUEST_DATE, 'MM') AS MONTH_KEY, COUNT(*) AS CNT "
            f'FROM "{table}" {clause} '
            "GROUP BY TRUNC(REQUEST_DATE, 'MM') "
            "ORDER BY MONTH_KEY ASC"
        )
        binds.update(win_binds)
        return sql, binds

    if intent.expire and intent.agg in (None, "count"):
        where = ""
        if intent.explicit_dates:
            where = "WHERE END_DATE BETWEEN :date_start AND :date_end"
            binds["date_start"] = intent.explicit_dates["start"]
            binds["date_end"] = intent.explicit_dates["end"]
        sql = f'SELECT COUNT(*) AS CNT FROM "{table}" {where}'.strip()
        return sql, binds

    if intent.group_by == "ENTITY" and intent.agg == "count":
        sql = (
            f"SELECT NVL(ENTITY, '(Unknown)') AS GROUP_KEY, COUNT(*) AS CNT "
            f'FROM "{table}" '
            "GROUP BY NVL(ENTITY, '(Unknown)') "
            "ORDER BY CNT DESC"
        )
        return sql, binds

    if intent.group_by == "CONTRACT_STATUS" and intent.agg == "count":
        sql = (
            "SELECT CONTRACT_STATUS AS GROUP_KEY, COUNT(*) AS CNT "
            f'FROM "{table}" GROUP BY CONTRACT_STATUS ORDER BY CNT DESC'
        )
        return sql, binds

    if intent.agg == "avg" and intent.group_by == "REQUEST_DATE":
        where, win_binds = _window_clause(intent, strict_overlap=True)
        clause = f"WHERE {where}" if where else ""
        sql = (
            "SELECT REQUEST_TYPE AS GROUP_KEY, AVG("
            f"{gross_sql()}) AS AVG_GROSS "
            f'FROM "{table}" {clause} '
            "GROUP BY REQUEST_TYPE ORDER BY AVG_GROSS DESC"
        )
        binds.update(win_binds)
        return sql, binds

    if intent.group_by == "OWNER_DEPARTMENT" and intent.agg in (None, "sum"):
        wants_gross = "VAT" in (intent.measure_sql or "")
        if wants_gross:
            window = None
            if intent.explicit_dates:
                window = (":date_start", ":date_end")
                binds["date_start"] = intent.explicit_dates["start"]
                binds["date_end"] = intent.explicit_dates["end"]
            sql = sql_total_gross_by_owner_department(
                window=window,
                strict_overlap=strict_overlap,
            )
            return sql, binds
        where, win_binds = _window_clause(intent, strict_overlap=strict_overlap)
        clause = f"WHERE {where}" if where else ""
        sql = (
            "SELECT OWNER_DEPARTMENT AS GROUP_KEY, SUM("
            f"{net_sql()}) AS MEASURE "
            f'FROM "{table}" {clause} '
            "GROUP BY OWNER_DEPARTMENT ORDER BY MEASURE DESC"
        )
        binds.update(win_binds)
        return sql, binds

    if stakeholders_union or "stakeholder" in intent.question.lower():
        where, win_binds = _window_clause(intent, strict_overlap=True)
        clause = f"WHERE {where}" if where else ""
        selects = [
            f"SELECT CONTRACT_STAKEHOLDER_{i} AS STK, {gross_sql()} AS G FROM \"{table}\" {clause}"
            for i in range(1, len(STAKEHOLDER_COLS) + 1)
        ]
        union_sql = " UNION ALL ".join(selects)
        sql = (
            "WITH U AS (" + union_sql + ") "
            "SELECT STK AS GROUP_KEY, SUM(G) AS MEASURE FROM U "
            "WHERE STK IS NOT NULL GROUP BY STK ORDER BY MEASURE DESC"
        )
        if intent.top_n:
            sql += " FETCH FIRST :top_n ROWS ONLY"
            binds["top_n"] = intent.top_n
        binds.update(win_binds)
        return sql, binds

    where, win_binds = _window_clause(intent, strict_overlap=True)
    clause = f"WHERE {where}" if where else ""
    select_list = _select_star(intent)
    if intent.sort_by:
        order = intent.sort_by
    elif intent.date_column == "REQUEST_DATE":
        order = "REQUEST_DATE"
    else:
        order = intent.measure_sql or net_sql()
    order_clause = f"ORDER BY {order} DESC" if intent.sort_desc else f"ORDER BY {order} ASC"
    fetch = _maybe_fetch_top(intent)
    sql = f'SELECT {select_list} FROM "{table}" {clause} {order_clause} {fetch}'.strip()
    binds.update(win_binds)
    _bind_top(intent, binds)
    return sql, binds
