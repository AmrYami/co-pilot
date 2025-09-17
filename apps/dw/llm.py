from __future__ import annotations

import re
from typing import Optional

from core.settings import Settings
from core.model_loader import load_llm
from core.text_utils import strip_code_fences


_LLM = None


def get_llm():
    """Lazy-load the base LLM (SQLCoder via ExLlama)."""
    global _LLM
    if _LLM is not None:
        return _LLM
    settings = Settings()
    try:
        _LLM = load_llm(settings)
        print("[dw][llm] Base LLM loaded (SQLCoder/ExLlama).")
    except Exception as exc:  # pragma: no cover - best effort logging
        print(f"[dw][llm] failed to load base LLM: {exc}")
        _LLM = None
    return _LLM


_SQL_ONLY_RE = re.compile(r"^\s*(SELECT|WITH)\b", re.IGNORECASE)


def _sanitize_sql(text: str) -> str:
    if "```" in text:
        text = strip_code_fences(text) if "strip_code_fences" in globals() else text.replace("```", "")
    semi = text.find(";")
    if semi != -1:
        text = text[:semi]
    forbidden = ("INSERT", "UPDATE", "DELETE", "MERGE", "DROP", "ALTER", "TRUNCATE", "CREATE")
    upper = text.strip().upper()
    if any(upper.startswith(tok) for tok in forbidden):
        return ""
    return text.strip()


def _oracle_guard(sql: str) -> bool:
    return bool(_SQL_ONLY_RE.match(sql or ""))


_ORACLE_HEADER = """-- Dialect: Oracle 19c+
-- Output ONLY valid Oracle SQL (SELECT or WITH). No comments after this line.
"""

_PROMPT = """You are a senior data engineer who writes precise Oracle SQL for analytics.
Rules:
- Output ONLY one Oracle SQL query (SELECT or WITH). No commentary.
- Use the provided table/columns only. Quote identifiers like "Contract" if needed.
- For amounts use NVL to treat NULL as 0.
- Never write INSERT/UPDATE/DELETE/DDL.
- Prefer bind placeholders (:date_start, :date_end, :top_n). Do not inline dates.

SCHEMA
------
Table: "Contract"
Important columns:
- CONTRACT_ID (NVARCHAR2)
- CONTRACT_OWNER (NVARCHAR2)
- CONTRACT_STAKEHOLDER_1..8 (NVARCHAR2)
- DEPARTMENT_1..8 (NVARCHAR2)   -- each paired with same index stakeholder
- OWNER_DEPARTMENT (NVARCHAR2)
- CONTRACT_VALUE_NET_OF_VAT (NUMBER)
- VAT (NUMBER)
- CONTRACT_PURPOSE, CONTRACT_SUBJECT (NVARCHAR2)
- START_DATE, END_DATE, REQUEST_DATE (DATE)
- ENTITY, ENTITY_NO (NVARCHAR2)
- REQUEST_ID, REQUEST_TYPE, REQUESTER (NVARCHAR2)
- CONTRACT_STATUS (NVARCHAR2)
- EXPIERY_30, EXPIERY_60, EXPIERY_90 (DATE)

Common patterns:
- Gross value = NVL(CONTRACT_VALUE_NET_OF_VAT,0) + NVL(VAT,0)
- “Last month” -> between :date_start (inclusive) and :date_end (exclusive)
- Top N -> use FETCH FIRST :top_n ROWS ONLY
- Stakeholder/Department pairs come from *_1..*_8 slots and are UNION ALL’ed

QUESTION
--------
{question}

REQUIREMENTS
------------
- Use bind parameters (:date_start, :date_end, :top_n) if a timeframe or limit is implied.
- If aggregating by stakeholder, normalize 8 pairs using UNION ALL over "Contract".
- List departments per stakeholder using LISTAGG(DISTINCT TRIM(DEPARTMENT), ', ') WITHIN GROUP (ORDER BY TRIM(DEPARTMENT)).
- Return readable column aliases.
"""


def nl_to_sql_with_llm(question: str) -> Optional[str]:
    llm = get_llm()
    if llm is None:
        return None
    prompt = _PROMPT.format(question=question.strip())
    try:
        out = llm.generate(prompt, stop=["</s>", "<|im_end|>"])
    except Exception as exc:  # pragma: no cover - runtime guard
        print(f"[dw][llm] generation error: {exc}")
        return None
    sql = _sanitize_sql(str(out or ""))
    if not sql or not _oracle_guard(sql):
        return None
    return sql
