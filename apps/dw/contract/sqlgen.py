from __future__ import annotations

"""Deterministic SQL generators for the Contract table."""


# Gross value expression used consistently across queries
GROSS_EXPR = (
    "NVL(CONTRACT_VALUE_NET_OF_VAT,0)"
    " + CASE WHEN NVL(VAT,0) BETWEEN 0 AND 1"
    "        THEN NVL(CONTRACT_VALUE_NET_OF_VAT,0) * NVL(VAT,0)"
    "        ELSE NVL(VAT,0) END"
)


def where_overlap() -> str:
    """Active window overlap between :date_start and :date_end."""

    return "(START_DATE <= :date_end AND END_DATE >= :date_start)"


def where_requested_window() -> str:
    """Requested window on REQUEST_DATE."""

    return "REQUEST_DATE BETWEEN :date_start AND :date_end"


def where_expiring_window() -> str:
    """Expiring window on END_DATE."""

    return "END_DATE BETWEEN :date_start AND :date_end"


def top_contracts_by_net(order_limit_bind: bool = True) -> str:
    sql = (
        'SELECT * FROM "Contract"\n'
        f"WHERE {where_overlap()}\n"
        "ORDER BY NVL(CONTRACT_VALUE_NET_OF_VAT,0) DESC\n"
    )
    if order_limit_bind:
        sql += "FETCH FIRST :top_n ROWS ONLY"
    return sql


def top_contracts_by_gross(order_limit_bind: bool = True) -> str:
    sql = (
        'SELECT * FROM "Contract"\n'
        f"WHERE {where_overlap()}\n"
        f"ORDER BY {GROSS_EXPR} DESC\n"
    )
    if order_limit_bind:
        sql += "FETCH FIRST :top_n ROWS ONLY"
    return sql


def list_requested_basic_columns() -> str:
    return (
        'SELECT CONTRACT_ID, CONTRACT_OWNER, REQUEST_DATE\n'
        'FROM "Contract"\n'
        f"WHERE {where_requested_window()}\n"
        "ORDER BY REQUEST_DATE DESC"
    )


def gross_per_owner_department_last_window() -> str:
    return (
        'SELECT\n'
        '  OWNER_DEPARTMENT AS GROUP_KEY,\n'
        f'  SUM({GROSS_EXPR}) AS MEASURE\n'
        'FROM "Contract"\n'
        f"WHERE {where_overlap()}\n"
        'GROUP BY OWNER_DEPARTMENT\n'
        'ORDER BY MEASURE DESC'
    )


def gross_per_owner_department_all_time() -> str:
    return (
        'SELECT\n'
        '  OWNER_DEPARTMENT AS GROUP_KEY,\n'
        f'  SUM({GROSS_EXPR}) AS MEASURE\n'
        'FROM "Contract"\n'
        'GROUP BY OWNER_DEPARTMENT\n'
        'ORDER BY MEASURE DESC'
    )


def status_counts_all_time() -> str:
    return (
        'SELECT\n'
        '  CONTRACT_STATUS AS GROUP_KEY,\n'
        '  COUNT(*) AS CNT\n'
        'FROM "Contract"\n'
        'GROUP BY CONTRACT_STATUS\n'
        'ORDER BY CNT DESC'
    )


def expiring_count_30d() -> str:
    return (
        'SELECT COUNT(*) AS CNT\n'
        'FROM "Contract"\n'
        f"WHERE {where_expiring_window()}"
    )


def expiring_list_window() -> str:
    return (
        'SELECT *\n'
        'FROM "Contract"\n'
        f"WHERE {where_expiring_window()}\n"
        'ORDER BY END_DATE ASC'
    )


def vat_zero_net_positive() -> str:
    return (
        'SELECT *\n'
        'FROM "Contract"\n'
        'WHERE NVL(VAT,0) = 0 AND NVL(CONTRACT_VALUE_NET_OF_VAT,0) > 0\n'
        'ORDER BY NVL(CONTRACT_VALUE_NET_OF_VAT,0) DESC'
    )


def requested_type_in_year(req_type_literal: str = "Renewal") -> str:
    return (
        'SELECT *\n'
        'FROM "Contract"\n'
        f"WHERE REQUEST_TYPE = '{req_type_literal}' "
        f"AND {where_requested_window()}\n"
        'ORDER BY REQUEST_DATE DESC'
    )


def entity_counts() -> str:
    return (
        'SELECT ENTITY AS GROUP_KEY, COUNT(*) AS CNT\n'
        'FROM "Contract"\n'
        'GROUP BY ENTITY\n'
        'ORDER BY CNT DESC'
    )


def owner_department_counts() -> str:
    return (
        'SELECT OWNER_DEPARTMENT AS GROUP_KEY, COUNT(*) AS CNT\n'
        'FROM "Contract"\n'
        'GROUP BY OWNER_DEPARTMENT\n'
        'ORDER BY CNT DESC'
    )


def owner_vs_oul_mismatch() -> str:
    return (
        'SELECT OWNER_DEPARTMENT, DEPARTMENT_OUL, COUNT(*) AS CNT\n'
        'FROM "Contract"\n'
        'WHERE DEPARTMENT_OUL IS NOT NULL\n'
        "  AND NVL(TRIM(OWNER_DEPARTMENT),'(None)') <> NVL(TRIM(DEPARTMENT_OUL),'(None)')\n"
        'GROUP BY OWNER_DEPARTMENT, DEPARTMENT_OUL\n'
        'ORDER BY CNT DESC'
    )

