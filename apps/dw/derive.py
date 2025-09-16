"""DocuWare-specific SQL derivations for simple contract analytics."""

from __future__ import annotations

import re


def _unpivot_contracts(alias: str = "c") -> str:
    """Return an inline UNION ALL that normalises stakeholder/department pairs."""

    base = f"""
      SELECT
        {alias}.DWDOCID                            AS DWDOCID,
        {alias}.CONTRACT_ID                        AS CONTRACT_ID,
        {alias}.CONTRACT_OWNER                     AS CONTRACT_OWNER,
        {alias}.OWNER_DEPARTMENT                   AS OWNER_DEPARTMENT,
        {alias}.CONTRACT_VALUE_NET_OF_VAT          AS CONTRACT_VALUE_NET_OF_VAT,
        {alias}.VAT                                AS VAT,
        NVL({alias}.CONTRACT_VALUE_NET_OF_VAT,0) + NVL({alias}.VAT,0) AS CONTRACT_VALUE_GROSS,
        {alias}.START_DATE                         AS START_DATE,
        {alias}.END_DATE                           AS END_DATE,
        {alias}.REQUEST_DATE                       AS REQUEST_DATE,
        {alias}.CONTRACT_STATUS                    AS CONTRACT_STATUS,
        {alias}.REQUEST_TYPE                       AS REQUEST_TYPE,
        {alias}.ENTITY_NO                          AS ENTITY_NO,
        {alias}.DEPARTMENT_OUL                     AS DEPARTMENT_OUL,
        :SLOT                                      AS SLOT,
        :STAKE                                     AS STAKEHOLDER,
        :DEPT                                      AS DEPARTMENT
      FROM "Contract" {alias}
      WHERE :STAKE IS NOT NULL
    """

    def select_for(slot: str, stake_col: str, dept_col: str) -> str:
        query = base
        query = query.replace(":SLOT", f"'{slot}'")
        query = query.replace(":STAKE", f"{alias}.{stake_col}")
        query = query.replace(":DEPT", f"{alias}.{dept_col}")
        return query

    parts = [
        select_for("1", "CONTRACT_STAKEHOLDER_1", "DEPARTMENT_1"),
        select_for("2", "CONTRACT_STAKEHOLDER_2", "DEPARTMENT_2"),
        select_for("3", "CONTRACT_STAKEHOLDER_3", "DEPARTMENT_3"),
        select_for("4", "CONTRACT_STAKEHOLDER_4", "DEPARTMENT_4"),
        select_for("5", "CONTRACT_STAKEHOLDER_5", "DEPARTMENT_5"),
        select_for("6", "CONTRACT_STAKEHOLDER_6", "DEPARTMENT_6"),
        select_for("7", "CONTRACT_STAKEHOLDER_7", "DEPARTMENT_7"),
        select_for("8", "CONTRACT_STAKEHOLDER_8", "DEPARTMENT_8"),
    ]
    return "\nUNION ALL\n".join(parts)


def top_departments_by_value_sql(limit: int = 10, months: int = 12) -> str:
    """Aggregate gross contract value by department over a rolling window."""

    unpivot = _unpivot_contracts(alias="c")
    return f"""
    WITH C AS (
      {unpivot}
    ), C2 AS (
      SELECT
        DEPARTMENT,
        CONTRACT_VALUE_GROSS,
        COALESCE(START_DATE, REQUEST_DATE) AS START_OR_REQ_DATE
      FROM C
    )
    SELECT
      DEPARTMENT,
      SUM(CONTRACT_VALUE_GROSS) AS TOTAL_VALUE
    FROM C2
    WHERE START_OR_REQ_DATE >= ADD_MONTHS(TRUNC(SYSDATE, 'MM'), -{int(months)})
    GROUP BY DEPARTMENT
    ORDER BY TOTAL_VALUE DESC
    FETCH FIRST {int(limit)} ROWS ONLY
    """


def top_stakeholders_by_value_sql(limit: int = 10, months: int = 12) -> str:
    """Aggregate gross contract value by stakeholder over a rolling window."""

    unpivot = _unpivot_contracts(alias="c")
    return f"""
    WITH C AS (
      {unpivot}
    ), C2 AS (
      SELECT
        STAKEHOLDER,
        CONTRACT_VALUE_GROSS,
        COALESCE(START_DATE, REQUEST_DATE) AS START_OR_REQ_DATE
      FROM C
    )
    SELECT
      STAKEHOLDER,
      SUM(CONTRACT_VALUE_GROSS) AS TOTAL_VALUE
    FROM C2
    WHERE START_OR_REQ_DATE >= ADD_MONTHS(TRUNC(SYSDATE, 'MM'), -{int(months)})
    GROUP BY STAKEHOLDER
    ORDER BY TOTAL_VALUE DESC
    FETCH FIRST {int(limit)} ROWS ONLY
    """


def route_question_to_sql(question: str) -> str | None:
    """Map a natural language question to a canned Oracle SQL statement."""

    ql = (question or "").lower()
    match = re.search(r"\btop\s+(\d+)\b", ql)
    limit = int(match.group(1)) if match else 10

    months = 12
    if "last month" in ql:
        months = 1
    elif "last 3" in ql and "month" in ql:
        months = 3
    elif "last 6" in ql:
        months = 6
    elif any(token in ql for token in ["last year", "past year", "12 months"]):
        months = 12

    if "department" in ql:
        return top_departments_by_value_sql(limit=limit, months=months)

    if "stakeholder" in ql:
        return top_stakeholders_by_value_sql(limit=limit, months=months)

    return None

