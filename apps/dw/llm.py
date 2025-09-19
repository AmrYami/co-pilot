import re

from core.model_loader import get_model

_SQL_STRIP_PATTERNS = [
    r"(?is)^sql\s*:\s*",
    r"(?is)^--.*?$",
    r"(?is)^/\*.*?\*/\s*",
]


def _normalize_sql(sql_text: str) -> str:
    if not sql_text:
        return ""
    s = sql_text.strip()
    for pat in _SQL_STRIP_PATTERNS:
        s = re.sub(pat, "", s).strip()
    match = re.search(r"(?is)\b(with|select)\b", s)
    if match:
        s = s[match.start() :].strip()
    if ";" in s:
        s = s.split(";", 1)[0].strip()
    return s

_SQL_SYSTEM_PROMPT = """Return ONLY Oracle SQL. No prose. No comments. SELECT or WITH only.
Use only table "Contract".
Allowed columns:
  CONTRACT_ID, CONTRACT_OWNER,
  CONTRACT_STAKEHOLDER_1, CONTRACT_STAKEHOLDER_2, CONTRACT_STAKEHOLDER_3, CONTRACT_STAKEHOLDER_4,
  CONTRACT_STAKEHOLDER_5, CONTRACT_STAKEHOLDER_6, CONTRACT_STAKEHOLDER_7, CONTRACT_STAKEHOLDER_8,
  DEPARTMENT_1, DEPARTMENT_2, DEPARTMENT_3, DEPARTMENT_4, DEPARTMENT_5, DEPARTMENT_6, DEPARTMENT_7, DEPARTMENT_8,
  OWNER_DEPARTMENT, CONTRACT_VALUE_NET_OF_VAT, VAT, CONTRACT_PURPOSE, CONTRACT_SUBJECT,
  START_DATE, END_DATE, REQUEST_DATE, REQUEST_TYPE, CONTRACT_STATUS, ENTITY_NO, REQUESTER.
Do NOT add any date filter unless the user explicitly asks (e.g., "next 30 days", "last month",
"between ... and ...", "since 2024-01-01").
If a window is asked and the user names a date column, use that column. Otherwise use REQUEST_DATE only when the user
explicitly says "request date".
Use named binds only from this whitelist: :date_start, :date_end, :top_n, :owner_name, :dept, :entity_no, :contract_id_pattern, :request_type.
Never invent other binds. Never bind obvious literals like 0, 1, 'ACTIVE' — write literals directly.
Use Oracle syntax: NVL(), TRIM(), UPPER(), LISTAGG(... WITHIN GROUP (...)), FETCH FIRST N ROWS ONLY.
"""


def _sanitize_oracle_select(raw: str) -> str | None:
    """Extract the first SELECT/WITH statement, strip comments/instructions, forbid DML/DDL."""

    if not raw:
        return None
    start = None
    lines = raw.splitlines()
    for idx, ln in enumerate(lines):
        s = ln.strip().lower()
        if s.startswith("select") or s.startswith("with"):
            start = idx
            break
    if start is None:
        return None
    sql_text = "\n".join(lines[start:]).strip()
    sql_text = sql_text.split(";", 1)[0].strip()
    if re.search(r"\b(insert|update|delete|merge|create|alter|drop|truncate|grant|revoke)\b", sql_text, re.I):
        return None
    if not re.match(r"^\s*(select|with)\b", sql_text, re.I):
        return None
    return sql_text


def nl_to_sql_with_llm(question: str, context: dict) -> str | None:
    """Ask the SQL model for Oracle SQL; return sanitized SELECT/CTE or None."""

    mdl = get_model("sql")
    prompt = f"{_SQL_SYSTEM_PROMPT}\n\nQuestion:\n{question}\nSQL:"
    raw = mdl.generate(
        prompt,
        max_new_tokens=256,
        stop=[],
    )
    normalized = _normalize_sql(raw)
    return _sanitize_oracle_select(normalized)


__all__ = [
    "_SQL_SYSTEM_PROMPT",
    "_SQL_STRIP_PATTERNS",
    "_normalize_sql",
    "_sanitize_oracle_select",
    "nl_to_sql_with_llm",
]
