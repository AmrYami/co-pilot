from __future__ import annotations

from typing import Tuple

from core.nlu.schema import NLIntent

STAKEHOLDER_COLS = [f"CONTRACT_STAKEHOLDER_{i}" for i in range(1, 9)]


def _unpivot_stakeholders_cte(table: str = '"Contract"') -> str:
    selects = []
    for i in range(1, 9):
        selects.append(
            "SELECT CONTRACT_ID, REQUEST_DATE, END_DATE, CONTRACT_OWNER, "
            "NVL(CONTRACT_VALUE_NET_OF_VAT,0) AS NET_VALUE, NVL(VAT,0) AS VAT, "
            f"CONTRACT_STAKEHOLDER_{i} AS STAKEHOLDER "
            f"FROM {table}"
        )
    return "WITH S AS (\n  " + "\n  UNION ALL\n  ".join(selects) + "\n)"


def _gross_expr() -> str:
    return "NET_VALUE + CASE WHEN VAT BETWEEN 0 AND 1 THEN NET_VALUE * VAT ELSE VAT END"


def build_sql(intent: NLIntent, table: str = '"Contract"') -> Tuple[str, dict]:
    binds: dict[str, object] = {}
    dc = intent.date_column or "REQUEST_DATE"

    where = []
    if intent.explicit_dates and intent.explicit_dates.start and intent.explicit_dates.end:
        where.append(f"{dc} BETWEEN :date_start AND :date_end")
        binds["date_start"] = intent.explicit_dates.start
        binds["date_end"] = intent.explicit_dates.end

    use_unpivot = bool(intent.group_by and intent.group_by.upper().startswith("CONTRACT_STAKEHOLDER"))
    cte = _unpivot_stakeholders_cte(table) if use_unpivot else ""

    source = "S" if use_unpivot else table
    measure = intent.measure_sql or "NVL(CONTRACT_VALUE_NET_OF_VAT,0)"
    if use_unpivot and "NVL(CONTRACT_VALUE_NET_OF_VAT,0)" in measure:
        notes_q = (intent.notes or {}).get("q", "")
        if "gross" in notes_q.lower():
            measure = _gross_expr()
        else:
            measure = "NET_VALUE"

    if intent.agg == "count" and intent.group_by:
        sel = f"SELECT {intent.group_by} AS GROUP_KEY, COUNT(*) AS CNT FROM {source}"
        grp = f"GROUP BY {intent.group_by}"
        cols = ["GROUP_KEY", "CNT"]
    elif intent.agg == "count":
        sel = f"SELECT COUNT(*) AS CNT FROM {source}"
        grp = ""
        cols = ["CNT"]
    elif intent.group_by:
        sel = f"SELECT {intent.group_by} AS GROUP_KEY, SUM({measure}) AS TOTAL FROM {source}"
        grp = f"GROUP BY {intent.group_by}"
        cols = ["GROUP_KEY", "TOTAL"]
    else:
        if intent.wants_all_columns:
            sel = f"SELECT * FROM {source}"
            cols = ["*"]
        else:
            sel = (
                f"SELECT CONTRACT_ID, CONTRACT_OWNER, {dc} AS WINDOW_DATE, "
                f"{measure} AS MEASURE FROM {source}"
            )
            cols = ["CONTRACT_ID", "CONTRACT_OWNER", "WINDOW_DATE", "MEASURE"]
        grp = ""

    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    order_sql = ""
    if intent.top_n:
        order_target = intent.sort_by or (
            "TOTAL" if intent.group_by else ("MEASURE" if not intent.wants_all_columns else dc)
        )
        direction = "DESC" if (intent.sort_desc is None or intent.sort_desc) else "ASC"
        order_sql = f"ORDER BY {order_target} {direction}"

    limit_sql = ""
    if intent.user_requested_top_n and intent.top_n:
        limit_sql = "FETCH FIRST :top_n ROWS ONLY"
        binds["top_n"] = intent.top_n

    parts = [cte, sel, where_sql, grp, order_sql, limit_sql]
    sql = "\n".join([p for p in parts if p]).strip()
    return sql, binds
