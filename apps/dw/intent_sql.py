from __future__ import annotations

from typing import Tuple


def _gross_sql() -> str:
    """Return SQL expression for gross contract value."""
    return (
        "NVL(CONTRACT_VALUE_NET_OF_VAT,0) + CASE WHEN NVL(VAT,0) BETWEEN 0 AND 1 "
        "THEN NVL(CONTRACT_VALUE_NET_OF_VAT,0) * NVL(VAT,0) ELSE NVL(VAT,0) END"
    )


def _net_sql() -> str:
    """Return SQL expression for net contract value."""
    return "NVL(CONTRACT_VALUE_NET_OF_VAT,0)"


def _stakeholder_unpivot(table: str, slots: int) -> str:
    """UNPIVOT helper that consolidates stakeholder slots into a single column."""
    cols = ",\n    ".join([f"CONTRACT_STAKEHOLDER_{i}" for i in range(1, slots + 1)])
    return f"""
    SELECT
      t.*,
      u.STAKEHOLDER
    FROM "{table}" t
    UNPIVOT (
      STAKEHOLDER FOR STAKEHOLDER_SLOT IN (
        {cols}
      )
    ) u
    """


def build_grouped_stakeholder_sql(
    table: str = "Contract",
    date_col: str = "REQUEST_DATE",
    top_n_bind: str = ":top_n",
    window_start_bind: str = ":date_start",
    window_end_bind: str = ":date_end",
    gross: bool = False,
    slots: int = 8,
) -> Tuple[str, str]:
    """
    Build summary and detail SQL for top-N stakeholder questions.

    Returns a tuple of (summary_sql, details_sql).
    """

    measure = _gross_sql() if gross else _net_sql()
    stk_cte = _stakeholder_unpivot(table, slots)

    summary_sql = f"""
    WITH stk AS (
      {stk_cte}
    ),
    agg AS (
      SELECT
        STAKEHOLDER AS GROUP_KEY,
        SUM({measure}) AS MEASURE_VALUE
      FROM stk
      WHERE {date_col} BETWEEN {window_start_bind} AND {window_end_bind}
        AND STAKEHOLDER IS NOT NULL
      GROUP BY STAKEHOLDER
      ORDER BY MEASURE_VALUE DESC
      FETCH FIRST {top_n_bind} ROWS ONLY
    )
    SELECT GROUP_KEY, MEASURE_VALUE
    FROM agg
    ORDER BY MEASURE_VALUE DESC
    """

    slot_or = " OR ".join(
        [f't.CONTRACT_STAKEHOLDER_{i} = a.GROUP_KEY' for i in range(1, slots + 1)]
    )
    details_sql = f"""
    WITH stk AS (
      {stk_cte}
    ),
    agg AS (
      SELECT
        STAKEHOLDER AS GROUP_KEY,
        SUM({measure}) AS MEASURE_VALUE
      FROM stk
      WHERE {date_col} BETWEEN {window_start_bind} AND {window_end_bind}
        AND STAKEHOLDER IS NOT NULL
      GROUP BY STAKEHOLDER
      ORDER BY MEASURE_VALUE DESC
      FETCH FIRST {top_n_bind} ROWS ONLY
    )
    SELECT t.*
    FROM "{table}" t
    JOIN agg a
      ON ({slot_or})
    WHERE t.{date_col} BETWEEN {window_start_bind} AND {window_end_bind}
    ORDER BY a.MEASURE_VALUE DESC, t.{date_col} DESC
    """

    return summary_sql.strip(), details_sql.strip()
