"""Prompt utilities for DocuWare SQL generation."""

CONTRACT_SCHEMA_SUMMARY = """
You are converting natural language to **Oracle SQL** for a single table "Contract".
Table: "Contract"
Important columns (only these matter for now):
- CONTRACT_ID (NVARCHAR2)
- CONTRACT_OWNER (NVARCHAR2)
- CONTRACT_STAKEHOLDER_1..8 (NVARCHAR2)
- DEPARTMENT_1..8 (NVARCHAR2)   -- DEPARTMENT_i corresponds to CONTRACT_STAKEHOLDER_i
- OWNER_DEPARTMENT (NVARCHAR2)
- CONTRACT_VALUE_NET_OF_VAT (NUMBER(27,2))
- VAT (NUMBER(27,2))
- START_DATE (DATE)
- END_DATE (DATE)
- REQUEST_DATE (DATE)
- CONTRACT_STATUS (NVARCHAR2)
- REQUEST_TYPE (NVARCHAR2)
- DEPARTMENT_OUL (NVARCHAR2) -- lead / manager department
- CONTRACTOR_ID (NVARCHAR2)
- REQUESTER (NVARCHAR2)
- ENTITY_NO (NVARCHAR2)

Derived:
- CONTRACT_VALUE_GROSS = NVL(CONTRACT_VALUE_NET_OF_VAT,0) + NVL(VAT,0)

Date column guidance:
- Unless the user explicitly specifies a different date column (e.g. END_DATE, START_DATE), default to REQUEST_DATE for time windows.
- For day-based windows, use half-open comparisons like END_DATE >= :date_start AND END_DATE < :date_end + 1. Avoid wrapping date columns in functions unless absolutely necessary.

Date windows (don’t use sysdate literals directly in SQL; caller binds :date_start, :date_end):
- last month: [:date_start, :date_end + 1) == previous calendar month
- last 90 days: [:date_start, :date_end + 1) rolling
- next 30 days: (today, today+30]

General rules:
- Output **SQL only** (no commentary). Valid Oracle SQL. SELECT/CTE only.
- Use **LISTAGG(..., ', ') WITHIN GROUP (ORDER BY ...)** to combine departments when needed.
- Use **FETCH FIRST :top_n ROWS ONLY** for top-N.
- Use bind names the caller sets: :date_start, :date_end, :top_n (when needed).
- Trim whitespace around NVARCHAR2 when grouping/displaying names.

Stakeholders unpivot rule (no views available):
Produce a CTE that UNION ALLs the 8 slots, e.g.
WITH stakeholders AS (
  SELECT CONTRACT_ID,
         NVL(CONTRACT_VALUE_NET_OF_VAT,0) + NVL(VAT,0) AS CONTRACT_VALUE_GROSS,
         CONTRACT_STAKEHOLDER_1 AS STAKEHOLDER,
         DEPARTMENT_1          AS DEPARTMENT,
         REQUEST_DATE          AS REF_DATE
  FROM "Contract"
  UNION ALL
  ...
  SELECT ..._8 ...
)
Then aggregate on TRIM(STAKEHOLDER) where needed.
"""

FEWSHOTS = [
    (
        "top 10 stakeholders by contract value last month",
        """WITH stakeholders AS (
  SELECT CONTRACT_ID,
         NVL(CONTRACT_VALUE_NET_OF_VAT,0) + NVL(VAT,0) AS CONTRACT_VALUE_GROSS,
         CONTRACT_STAKEHOLDER_1 AS STAKEHOLDER,
         DEPARTMENT_1 AS DEPARTMENT,
         REQUEST_DATE AS REF_DATE
  FROM "Contract"
  UNION ALL
  SELECT CONTRACT_ID, NVL(CONTRACT_VALUE_NET_OF_VAT,0)+NVL(VAT,0), CONTRACT_STAKEHOLDER_2, DEPARTMENT_2, REQUEST_DATE FROM "Contract"
  UNION ALL
  SELECT CONTRACT_ID, NVL(CONTRACT_VALUE_NET_OF_VAT,0)+NVL(VAT,0), CONTRACT_STAKEHOLDER_3, DEPARTMENT_3, REQUEST_DATE FROM "Contract"
  UNION ALL
  SELECT CONTRACT_ID, NVL(CONTRACT_VALUE_NET_OF_VAT,0)+NVL(VAT,0), CONTRACT_STAKEHOLDER_4, DEPARTMENT_4, REQUEST_DATE FROM "Contract"
  UNION ALL
  SELECT CONTRACT_ID, NVL(CONTRACT_VALUE_NET_OF_VAT,0)+NVL(VAT,0), CONTRACT_STAKEHOLDER_5, DEPARTMENT_5, REQUEST_DATE FROM "Contract"
  UNION ALL
  SELECT CONTRACT_ID, NVL(CONTRACT_VALUE_NET_OF_VAT,0)+NVL(VAT,0), CONTRACT_STAKEHOLDER_6, DEPARTMENT_6, REQUEST_DATE FROM "Contract"
  UNION ALL
  SELECT CONTRACT_ID, NVL(CONTRACT_VALUE_NET_OF_VAT,0)+NVL(VAT,0), CONTRACT_STAKEHOLDER_7, DEPARTMENT_7, REQUEST_DATE FROM "Contract"
  UNION ALL
  SELECT CONTRACT_ID, NVL(CONTRACT_VALUE_NET_OF_VAT,0)+NVL(VAT,0), CONTRACT_STAKEHOLDER_8, DEPARTMENT_8, REQUEST_DATE FROM "Contract"
)
SELECT
  TRIM(STAKEHOLDER) AS stakeholder,
  SUM(CONTRACT_VALUE_GROSS) AS total_gross_value,
  COUNT(DISTINCT CONTRACT_ID) AS contract_count,
  LISTAGG(DISTINCT TRIM(DEPARTMENT), ', ') WITHIN GROUP (ORDER BY TRIM(DEPARTMENT)) AS departments
FROM stakeholders
WHERE STAKEHOLDER IS NOT NULL
  AND TRIM(STAKEHOLDER) <> ''
  AND REF_DATE >= :date_start
  AND REF_DATE <  :date_end + 1
GROUP BY TRIM(STAKEHOLDER)
ORDER BY total_gross_value DESC
FETCH FIRST :top_n ROWS ONLY"""
    ),
    (
        "contracts expiring in the next 30 days",
        """SELECT
  CONTRACT_ID,
  CONTRACT_OWNER,
  OWNER_DEPARTMENT,
  END_DATE,
  CONTRACT_STATUS,
  NVL(CONTRACT_VALUE_NET_OF_VAT,0) + NVL(VAT,0) AS CONTRACT_VALUE_GROSS
FROM "Contract"
WHERE END_DATE >= :date_start
  AND END_DATE <  :date_end + 1
ORDER BY END_DATE ASC"""
    ),
    (
        "total gross contract value by owner department last 90 days",
        """SELECT
  TRIM(OWNER_DEPARTMENT) AS owner_department,
  SUM(NVL(CONTRACT_VALUE_NET_OF_VAT,0) + NVL(VAT,0)) AS total_gross_value,
  COUNT(*) AS contract_count
FROM "Contract"
WHERE REQUEST_DATE >= :date_start
  AND REQUEST_DATE <  :date_end + 1
GROUP BY TRIM(OWNER_DEPARTMENT)
ORDER BY total_gross_value DESC"""
    ),
]

SYSTEM_INSTRUCTIONS = (
    CONTRACT_SCHEMA_SUMMARY
    + "\nRespond with **SQL only**. No explanations. Use only the listed columns."
)
