from __future__ import annotations

import os
import re
import textwrap
from typing import Any, Dict, Optional, Tuple


def _load_sql_generator(settings: Optional[Any] = None):
    """Load the SQL generation model regardless of loader name variations."""
    try:
        import core.model_loader as model_loader
    except Exception as exc:  # pragma: no cover - critical import
        raise ImportError(f"Cannot import core.model_loader: {exc}") from exc

    candidates = [
        "load_llm",
        "load_primary",
        "load_base_generator",
        "load_base",
        "load_sql_model",
    ]

    for name in candidates:
        if hasattr(model_loader, name):
            loader = getattr(model_loader, name)
            try:
                return loader(settings=settings)
            except TypeError:
                return loader()

    raise ImportError(
        "No compatible loader function found in core.model_loader. "
        "Expected one of: " + ", ".join(candidates)
    )


def _gen_text(
    generator: Any,
    prompt: str,
    *,
    stop: Optional[list[str]] = None,
    max_new_tokens: Optional[int] = None,
    temperature: Optional[float] = None,
    top_p: Optional[float] = None,
) -> str:
    params: Dict[str, Any] = {}
    if stop is not None:
        params["stop"] = stop
    if max_new_tokens is not None:
        params["max_new_tokens"] = max_new_tokens
    if temperature is not None:
        params["temperature"] = temperature
    if top_p is not None:
        params["top_p"] = top_p

    if hasattr(generator, "generate") and callable(generator.generate):
        return generator.generate(prompt, **params)
    if callable(generator):
        return generator(prompt, **params)
    raise RuntimeError("LLM generator object is not callable and has no .generate().")


_ORACLE_RULES = """
Rules:
- SQL dialect: Oracle (LISTAGG, NVL, TRUNC, FETCH FIRST ... ROWS ONLY).
- Output ONLY a single CTE/SELECT statement. No DML/DDL. No comments. No semicolon.
- Prefer REQUEST_DATE as the default date if user says “last month/last 90 days”.
- If user asks “next N days” for END_DATE, filter on END_DATE with CURRENT_DATE + N.
- When aggregating stakeholder/department, normalize the 8 slots (1..8) with UNION ALL.
- Gross value = NVL(CONTRACT_VALUE_NET_OF_VAT,0) + NVL(VAT,0).
- Only reference existing columns; if a field is ambiguous, pick the closest match.
"""

_CONTRACT_COLUMNS = """
Table "Contract" (DocuWare):
- CONTRACT_ID (text): contract identifier (human-friendly).
- CONTRACT_OWNER (text): owner of the contract.
- CONTRACT_STAKEHOLDER_1..8 (text): stakeholder names per slot.
- DEPARTMENT_1..8 (text): department mapped 1:1 with each stakeholder slot.
- OWNER_DEPARTMENT (text): owner’s department.
- CONTRACT_VALUE_NET_OF_VAT (number): net value.
- VAT (number): VAT amount.
- CONTRACT_PURPOSE, CONTRACT_SUBJECT (text): descriptors.
- START_DATE, END_DATE, REQUEST_DATE (date): key dates.
- DURATION (text), ENTITY, BUILDING_AND_FLOOR_DESCRIPTION, LEGAL_NAME_OF_THE_COMPANY,
  REPRESENTATIVE_NAME/PHONE/EMAIL (text): contextual fields.
- CONTRACT_STATUS (text), REQUEST_TYPE (text), REQUESTER (text).
- DEPARTMENT_OUL (text): lead/manager of the departments.
- EXPIERY_30/60/90 (date): expiry thresholds.
- ENTITY_NO (text): entity reference.
"""

_FEWSHOTS = """
Q: top 10 stakeholders by contract value last month
SQL:
WITH base AS (
  SELECT
    CONTRACT_ID,
    NVL(CONTRACT_VALUE_NET_OF_VAT,0) + NVL(VAT,0) AS CONTRACT_VALUE_GROSS,
    REQUEST_DATE
  FROM "Contract"
  WHERE REQUEST_DATE >= ADD_MONTHS(TRUNC(CURRENT_DATE, 'MM'), -1)
    AND REQUEST_DATE <  TRUNC(CURRENT_DATE, 'MM')
), stakeholders AS (
  SELECT CONTRACT_ID, CONTRACT_VALUE_GROSS, CONTRACT_STAKEHOLDER_1 AS STAKEHOLDER FROM "Contract"
  UNION ALL SELECT CONTRACT_ID, CONTRACT_VALUE_GROSS, CONTRACT_STAKEHOLDER_2 FROM "Contract"
  UNION ALL SELECT CONTRACT_ID, CONTRACT_VALUE_GROSS, CONTRACT_STAKEHOLDER_3 FROM "Contract"
  UNION ALL SELECT CONTRACT_ID, CONTRACT_VALUE_GROSS, CONTRACT_STAKEHOLDER_4 FROM "Contract"
  UNION ALL SELECT CONTRACT_ID, CONTRACT_VALUE_GROSS, CONTRACT_STAKEHOLDER_5 FROM "Contract"
  UNION ALL SELECT CONTRACT_ID, CONTRACT_VALUE_GROSS, CONTRACT_STAKEHOLDER_6 FROM "Contract"
  UNION ALL SELECT CONTRACT_ID, CONTRACT_VALUE_GROSS, CONTRACT_STAKEHOLDER_7 FROM "Contract"
  UNION ALL SELECT CONTRACT_ID, CONTRACT_VALUE_GROSS, CONTRACT_STAKEHOLDER_8 FROM "Contract"
)
SELECT
  TRIM(STAKEHOLDER) AS stakeholder,
  SUM(CONTRACT_VALUE_GROSS) AS total_gross_value,
  COUNT(DISTINCT CONTRACT_ID) AS contract_count
FROM stakeholders
WHERE STAKEHOLDER IS NOT NULL AND TRIM(STAKEHOLDER) <> ''
GROUP BY TRIM(STAKEHOLDER)
ORDER BY total_gross_value DESC
FETCH FIRST 10 ROWS ONLY

Q: contracts expiring in the next 30 days
SQL:
SELECT
  CONTRACT_ID,
  END_DATE,
  CONTRACT_OWNER,
  OWNER_DEPARTMENT,
  NVL(CONTRACT_VALUE_NET_OF_VAT,0) + NVL(VAT,0) AS CONTRACT_VALUE_GROSS
FROM "Contract"
WHERE END_DATE >= TRUNC(CURRENT_DATE)
  AND END_DATE <  TRUNC(CURRENT_DATE) + 30
ORDER BY END_DATE ASC
"""


def _build_prompt(question: str) -> str:
    header = "You are a senior Oracle SQL analyst. Convert the question to a single valid Oracle SELECT (or CTE + SELECT)."
    prompt = "\n\n".join(
        [
            header,
            _ORACLE_RULES.strip(),
            _CONTRACT_COLUMNS.strip(),
            _FEWSHOTS.strip(),
            f"Q: {question}\nSQL:",
        ]
    )
    return textwrap.dedent(prompt).strip()


_FORBIDDEN = re.compile(
    r"\b(UPDATE|DELETE|INSERT|MERGE|CREATE|ALTER|DROP|TRUNCATE|GRANT|REVOKE)\b",
    re.IGNORECASE,
)


def _clean_and_validate(sql_text: str) -> str:
    sql = sql_text.strip()
    if "```" in sql:
        parts = sql.split("```")
        if len(parts) >= 2:
            sql = parts[1]
    sql = sql.strip()
    if sql.endswith(";"):
        sql = sql[:-1].strip()

    first_token = sql.split(None, 1)[0].upper() if sql else ""
    if first_token not in {"SELECT", "WITH"}:
        raise ValueError(
            f"Generated SQL must start with SELECT/WITH, got '{first_token or '(empty)'}'."
        )

    if _FORBIDDEN.search(sql):
        raise ValueError("Generated SQL contains forbidden statements.")

    return sql


def nl_to_sql_with_llm(
    question: str,
    *,
    max_new_tokens: Optional[int] = None,
) -> Tuple[str, str]:
    generator = _load_sql_generator()

    stop_env = os.getenv("STOP")
    stop_list = [item.strip() for item in stop_env.split(",") if item.strip()] if stop_env else None

    default_max_new_tokens = int(os.getenv("GENERATION_MAX_NEW_TOKENS", "256"))
    temperature = float(os.getenv("GENERATION_TEMPERATURE", "0.2"))
    top_p = float(os.getenv("GENERATION_TOP_P", "0.9"))

    prompt = _build_prompt(question)
    raw_output = _gen_text(
        generator,
        prompt,
        stop=stop_list,
        max_new_tokens=max_new_tokens or default_max_new_tokens,
        temperature=temperature,
        top_p=top_p,
    )

    sql = _clean_and_validate(str(raw_output or ""))
    rationale = "Generated via SQLCoder with Oracle rules and Contract schema context."
    return sql, rationale
