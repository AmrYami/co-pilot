"""Prompt construction helpers for DocuWare NL→SQL generation."""

from __future__ import annotations

from textwrap import dedent
from typing import Iterable, Sequence, Tuple

IMPORTANT_COLUMNS: Sequence[str] = (
    "CONTRACT_ID",
    "CONTRACT_OWNER",
    "CONTRACT_STAKEHOLDER_1",
    "CONTRACT_STAKEHOLDER_2",
    "CONTRACT_STAKEHOLDER_3",
    "CONTRACT_STAKEHOLDER_4",
    "CONTRACT_STAKEHOLDER_5",
    "CONTRACT_STAKEHOLDER_6",
    "CONTRACT_STAKEHOLDER_7",
    "CONTRACT_STAKEHOLDER_8",
    "DEPARTMENT_1",
    "DEPARTMENT_2",
    "DEPARTMENT_3",
    "DEPARTMENT_4",
    "DEPARTMENT_5",
    "DEPARTMENT_6",
    "DEPARTMENT_7",
    "DEPARTMENT_8",
    "OWNER_DEPARTMENT",
    "CONTRACT_VALUE_NET_OF_VAT",
    "VAT",
    "CONTRACT_PURPOSE",
    "CONTRACT_SUBJECT",
    "START_DATE",
    "END_DATE",
    "REQUEST_DATE",
    "ENTITY",
    "BUILDING_AND_FLOOR_DESCRIPTIO",
    "LEGAL_NAME_OF_THE_COMPANY",
    "REPRESENTATIVE_NAME",
    "REPRESENTATIVE_PHONE",
    "REPRESENTATIVE_EMAIL",
    "VALUE_DESCRIPTION",
    "YEAR",
    "CONTRACTOR_ID",
    "REQUEST_ID",
    "CONTRACT_STATUS",
    "REQUEST_TYPE",
    "DEPARTMENT_OUL",
    "CONTRACT_ID_COUNTER",
    "REQUESTER",
    "EXPIERY_30",
    "EXPIERY_60",
    "EXPIERY_90",
    "ENTITY_NO",
)


def _schema_card() -> str:
    cols = ", ".join(IMPORTANT_COLUMNS)
    return dedent(
        f"""
        You are an expert Oracle SQL generator. Use ONLY this table and columns:
        TABLE "Contract"({cols})

        NOTES:
        - "stakeholder" refers to any of CONTRACT_STAKEHOLDER_1..8
        - DEPARTMENT_1..8 correspond positionally to the stakeholder slots (1..8)
        - Gross value = NVL(CONTRACT_VALUE_NET_OF_VAT,0) + NVL(VAT,0)
        - Allowed operations: SELECT / CTE only. No INSERT/UPDATE/DELETE/DDL.
        - Use Oracle syntax:
            • string concat: ||
            • missing handling: NVL(col, 0)
            • list aggregation: LISTAGG(x, ', ') WITHIN GROUP (ORDER BY x)
            • limit: FETCH FIRST :top_n ROWS ONLY
            • bind params: :date_start, :date_end, :top_n, etc.
        - Quote table name as "Contract". Columns are upper-case (no quotes needed).
        - Time phrases:
            • "last month" = [first day of previous calendar month, first day of current month)
            • "last 30 days" = [SYSDATE-30, SYSDATE)
            • "next 30 days" = [SYSDATE, SYSDATE+30)
            • If no explicit field mentioned, prefer REQUEST_DATE for activity windows; for expiry use END_DATE; for active use START_DATE/END_DATE overlap logic.
        OUTPUT: Only a single Oracle SQL string. Nothing else.
        """
    ).strip()


def _seed_fewshots() -> str:
    return dedent(
        """
        Q: top 10 stakeholders by contract value last month
        A:
        WITH s AS (
          SELECT CONTRACT_ID,
                 NVL(CONTRACT_VALUE_NET_OF_VAT,0)+NVL(VAT,0) AS CONTRACT_VALUE_GROSS,
                 CONTRACT_STAKEHOLDER_1 AS STAKEHOLDER, DEPARTMENT_1 AS DEPARTMENT, REQUEST_DATE AS REF_DATE
            FROM "Contract"
          UNION ALL SELECT CONTRACT_ID, NVL(CONTRACT_VALUE_NET_OF_VAT,0)+NVL(VAT,0), CONTRACT_STAKEHOLDER_2, DEPARTMENT_2, REQUEST_DATE FROM "Contract"
          UNION ALL SELECT CONTRACT_ID, NVL(CONTRACT_VALUE_NET_OF_VAT,0)+NVL(VAT,0), CONTRACT_STAKEHOLDER_3, DEPARTMENT_3, REQUEST_DATE FROM "Contract"
          UNION ALL SELECT CONTRACT_ID, NVL(CONTRACT_VALUE_NET_OF_VAT,0)+NVL(VAT,0), CONTRACT_STAKEHOLDER_4, DEPARTMENT_4, REQUEST_DATE FROM "Contract"
          UNION ALL SELECT CONTRACT_ID, NVL(CONTRACT_VALUE_NET_OF_VAT,0)+NVL(VAT,0), CONTRACT_STAKEHOLDER_5, DEPARTMENT_5, REQUEST_DATE FROM "Contract"
          UNION ALL SELECT CONTRACT_ID, NVL(CONTRACT_VALUE_NET_OF_VAT,0)+NVL(VAT,0), CONTRACT_STAKEHOLDER_6, DEPARTMENT_6, REQUEST_DATE FROM "Contract"
          UNION ALL SELECT CONTRACT_ID, NVL(CONTRACT_VALUE_NET_OF_VAT,0)+NVL(VAT,0), CONTRACT_STAKEHOLDER_7, DEPARTMENT_7, REQUEST_DATE FROM "Contract"
          UNION ALL SELECT CONTRACT_ID, NVL(CONTRACT_VALUE_NET_OF_VAT,0)+NVL(VAT,0), CONTRACT_STAKEHOLDER_8, DEPARTMENT_8, REQUEST_DATE FROM "Contract"
        )
        SELECT TRIM(STAKEHOLDER) AS stakeholder,
               SUM(CONTRACT_VALUE_GROSS) AS total_gross_value,
               COUNT(DISTINCT CONTRACT_ID) AS contract_count,
               LISTAGG(DISTINCT TRIM(DEPARTMENT), ', ') WITHIN GROUP (ORDER BY TRIM(DEPARTMENT)) AS departments
          FROM s
         WHERE STAKEHOLDER IS NOT NULL AND TRIM(STAKEHOLDER) <> ''
           AND REF_DATE >= :date_start AND REF_DATE < :date_end
         GROUP BY TRIM(STAKEHOLDER)
         ORDER BY total_gross_value DESC
         FETCH FIRST :top_n ROWS ONLY

        Q: contracts expiring in the next 30 days
        A:
        SELECT CONTRACT_ID,
               CONTRACT_OWNER,
               OWNER_DEPARTMENT,
               END_DATE,
               CONTRACT_STATUS,
               NVL(CONTRACT_VALUE_NET_OF_VAT,0)+NVL(VAT,0) AS CONTRACT_VALUE_GROSS
          FROM "Contract"
         WHERE END_DATE >= :date_start AND END_DATE < :date_end
         ORDER BY END_DATE ASC
        """
    ).strip()


def _format_teach_shots(shots: Iterable[Tuple[str, str]] | None) -> str:
    if not shots:
        return ""
    formatted = []
    for question, sql in shots:
        if not question or not sql:
            continue
        formatted.append(
            dedent(
                f"""
                Q: {question.strip()}
                A:
                {sql.strip()}
                """
            ).strip()
        )
    return "\n\n".join(formatted)


def build_nl2sql_prompt(question: str, extra_shots: Iterable[Tuple[str, str]] | None = None) -> str:
    prompt_parts = [_schema_card(), _seed_fewshots()]
    extra = _format_teach_shots(extra_shots)
    if extra:
        prompt_parts.append(extra)
    prompt_parts.append(f"Q: {question}\nA:")
    return "\n\n".join(part.strip() for part in prompt_parts if part).strip()
