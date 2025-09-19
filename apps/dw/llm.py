import re
from typing import Any, Dict, Optional, Tuple

from core.model_loader import get_model

# Allowed binds for Oracle in this app
ALLOWED_BINDS = {"date_start", "date_end"}

# System prompt for SQLCoder (DW)
SQL_SYSTEM_PROMPT = """You write ONLY Oracle SQL for a DocuWare table named \"Contract\".
Rules:
- Output must be a single SELECT or WITH...SELECT statement. No comments, no prose, no prefixes/suffixes.
- Use ONLY these columns when relevant:
  CONTRACT_ID, CONTRACT_OWNER,
  CONTRACT_STAKEHOLDER_1, CONTRACT_STAKEHOLDER_2, CONTRACT_STAKEHOLDER_3, CONTRACT_STAKEHOLDER_4,
  CONTRACT_STAKEHOLDER_5, CONTRACT_STAKEHOLDER_6, CONTRACT_STAKEHOLDER_7, CONTRACT_STAKEHOLDER_8,
  DEPARTMENT_1, DEPARTMENT_2, DEPARTMENT_3, DEPARTMENT_4,
  DEPARTMENT_5, DEPARTMENT_6, DEPARTMENT_7, DEPARTMENT_8,
  OWNER_DEPARTMENT,
  CONTRACT_VALUE_NET_OF_VAT, VAT,
  CONTRACT_PURPOSE, CONTRACT_SUBJECT,
  START_DATE, END_DATE, REQUEST_DATE, REQUEST_TYPE, CONTRACT_STATUS,
  ENTITY_NO, REQUESTER.
- Oracle syntax: NVL(), LISTAGG(... WITHIN GROUP (...)), TRIM(), UPPER(), FETCH FIRST N ROWS ONLY.
- DO NOT create any bind variables unless the question explicitly asks for a time window and names a date column
  (e.g. \"END_DATE in next 30 days\"). Only then use :date_start and :date_end. NEVER invent other binds.
- If no time window is asked: do NOT use binds at all; write direct predicates.
- Never write DML/DDL; SELECT/CTE only.
"""


def _extract_select(sql_text: str) -> Optional[str]:
    """Return only the first SELECT/WITH statement; strip fences/prose."""
    if not sql_text:
        return None
    # remove code fences
    trimmed = re.sub(r"^```(?:sql)?\\s*|\\s*```$", "", sql_text, flags=re.IGNORECASE | re.MULTILINE)
    # find first SELECT or WITH
    match = re.search(r"(?is)\\b(SELECT|WITH)\\b.*", trimmed)
    if not match:
        return None
    cleaned = match.group(0).strip()
    cleaned = re.split(r"\n\\s*SQL\\s*:\\s*\n", cleaned, maxsplit=1)[0].strip()
    cleaned = cleaned.rstrip(";")
    return cleaned if cleaned.upper().startswith(("SELECT", "WITH")) else None


def _unexpected_binds(sql: str) -> set[str]:
    """Return set of bind names that are not allowed (:date_start/:date_end)."""
    binds = set(re.findall(r":([A-Za-z_]\\w*)", sql or ""))
    return {bind for bind in binds if bind not in ALLOWED_BINDS}


def nl_to_sql_with_llm(question: str, force_time_window: bool = False) -> Tuple[Optional[str], Dict[str, Any]]:
    """Generate Oracle SQL from natural language with strict bind/shape discipline."""
    model = get_model("sql")
    if model is None:
        return None, {"reason": "no_model"}

    meta: Dict[str, Any] = {"retries": 0}

    def _ask(system_prompt: str, user_prompt: str) -> Optional[str]:
        try:
            output = model.generate(system=system_prompt, user=user_prompt)
        except Exception as exc:  # pragma: no cover - model transport errors
            meta["reason"] = "error"
            meta["error"] = str(exc)
            return None
        return _extract_select(output)

    user_prompt = f"Question:\n{question}\nReturn ONLY the SQL."

    sql = _ask(SQL_SYSTEM_PROMPT, user_prompt)
    if not sql:
        meta.setdefault("reason", "not_select")
        return None, meta

    bad_binds = _unexpected_binds(sql)
    if bad_binds:
        meta["retries"] = 1
        strict_prompt = (
            SQL_SYSTEM_PROMPT
            + "\nABSOLUTE RULE: Do NOT invent binds. Only :date_start/:date_end are permitted and only if the question asks "
            "a time window on a named date column."
        )
        retry_sql = _ask(strict_prompt, user_prompt)
        if retry_sql:
            retry_bad = _unexpected_binds(retry_sql)
            if not retry_bad:
                meta["unexpected_binds_first_try"] = sorted(bad_binds)
                return retry_sql, meta
            sql = retry_sql
            bad_binds = retry_bad
        meta["reason"] = "unexpected_binds"
        meta["bad_binds"] = sorted(bad_binds)
        return None, meta

    return sql, meta


__all__ = ["ALLOWED_BINDS", "SQL_SYSTEM_PROMPT", "_extract_select", "_unexpected_binds", "nl_to_sql_with_llm"]
