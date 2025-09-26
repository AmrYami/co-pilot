from __future__ import annotations

from .base import TableSpec
from . import register

# All code and comments in English only

GROSS_EXPR = (
    "NVL(CONTRACT_VALUE_NET_OF_VAT,0) + "
    "CASE WHEN NVL(VAT,0) BETWEEN 0 AND 1 "
    "THEN NVL(CONTRACT_VALUE_NET_OF_VAT,0) * NVL(VAT,0) "
    "ELSE NVL(VAT,0) END"
)


def sql_total_gross_by_owner_department(window=None, strict_overlap=True) -> str:
    """Return SUM(gross) grouped by OWNER_DEPARTMENT."""

    where = []
    if window:
        start_bind, end_bind = window
        if strict_overlap:
            where.append(
                "(START_DATE IS NOT NULL AND END_DATE IS NOT NULL "
                f"AND START_DATE <= {end_bind} AND END_DATE >= {start_bind})"
            )
        else:
            where.append(
                "(COALESCE(START_DATE, DATE '1900-01-01') <= "
                f"{end_bind} AND COALESCE(END_DATE,   DATE '9999-12-31') >= {start_bind})"
            )
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    return f"""
SELECT
  NVL(OWNER_DEPARTMENT, '(Unknown)') AS GROUP_KEY,
  SUM({GROSS_EXPR}) AS MEASURE
FROM "Contract"
{where_sql}
GROUP BY NVL(OWNER_DEPARTMENT, '(Unknown)')
ORDER BY MEASURE DESC
""".strip()

# Natural-language → columns (what you told me)
_DIMENSIONS = {
    "contract":              "CONTRACT_ID",
    "contract id":           "CONTRACT_ID",
    "owner":                 "CONTRACT_OWNER",
    "contract owner":        "CONTRACT_OWNER",
    "owner department":      "OWNER_DEPARTMENT",
    "department":            "OWNER_DEPARTMENT",
    "manager":               "DEPARTMENT_OUL",
    "department_oul":        "DEPARTMENT_OUL",
    "entity":                "ENTITY",
    "entity no":             "ENTITY_NO",
    "stakeholder":           "CONTRACT_STAKEHOLDER_1",  # default slot; we can extend to 1..8 later
    "stakeholder1":          "CONTRACT_STAKEHOLDER_1",
    "status":                "CONTRACT_STATUS",
    "request type":          "REQUEST_TYPE",
    "request":               "REQUEST_TYPE",
}

ContractSpec = TableSpec(
    name="Contract",
    # Per your latest rule: default is OVERLAP unless user explicitly says "requested"
    default_date_mode="OVERLAP",
    request_date_col="REQUEST_DATE",
    start_date_col="START_DATE",
    end_date_col="END_DATE",
    value_col_net="CONTRACT_VALUE_NET_OF_VAT",
    value_col_vat="VAT",
    dimension_map=_DIMENSIONS,
    fts_default=[
        "CONTRACT_SUBJECT","CONTRACT_PURPOSE","OWNER_DEPARTMENT","DEPARTMENT_OUL",
        "CONTRACT_OWNER","CONTRACT_STAKEHOLDER_1","CONTRACT_STAKEHOLDER_2",
        "LEGAL_NAME_OF_THE_COMPANY","ENTITY","ENTITY_NO"
    ],
)

register(ContractSpec)
