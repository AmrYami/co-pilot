import re
from typing import Any, Dict, List, Optional

from core.model_loader import get_model

_BEGIN = "BEGIN_SQL"
_END = "END_SQL"

PROMPT = """You are an Oracle SQL expert.

Rules:
1) Output ONLY a valid Oracle SELECT or WITH ... SELECT. No prose, no comments.
2) Use ONLY table "Contract".
3) Allowed columns:
   CONTRACT_ID, CONTRACT_OWNER,
   CONTRACT_STAKEHOLDER_1, CONTRACT_STAKEHOLDER_2, CONTRACT_STAKEHOLDER_3, CONTRACT_STAKEHOLDER_4,
   CONTRACT_STAKEHOLDER_5, CONTRACT_STAKEHOLDER_6, CONTRACT_STAKEHOLDER_7, CONTRACT_STAKEHOLDER_8,
   DEPARTMENT_1, DEPARTMENT_2, DEPARTMENT_3, DEPARTMENT_4, DEPARTMENT_5, DEPARTMENT_6, DEPARTMENT_7, DEPARTMENT_8,
   OWNER_DEPARTMENT, CONTRACT_VALUE_NET_OF_VAT, VAT, CONTRACT_PURPOSE, CONTRACT_SUBJECT,
   START_DATE, END_DATE, REQUEST_DATE, REQUEST_TYPE, CONTRACT_STATUS, ENTITY_NO, REQUESTER.
4) Use Oracle syntax (NVL, TRIM, UPPER, LISTAGG ... WITHIN GROUP ..., FETCH FIRST N ROWS ONLY).
5) Named binds allowed ONLY if you use them explicitly in the SQL: :date_start, :date_end, :top_n, :owner_name, :dept, :entity_no, :contract_id_pattern, :request_type.
   - Do not invent other binds.
   - Do not bind obvious literals like 0 or 'ACTIVE'—write literals directly.
6) If the user explicitly asks for a time window (e.g., "last month", "next 30 days", "in 2024", "between ... and ..."):
   - Use the specified date column; if none specified, use REQUEST_DATE by default.
   - Use :date_start and :date_end binds for the window.
7) Never modify data (no DML/DDL). SELECT/CTE only.

User question:
{question}

Output:
{begin}
<SQL HERE>
{end}
"""

_FILTER_INSTRUCTIONS = {
    "owner_name": "Filter on CONTRACT_OWNER using bind :owner_name.",
    "dept": "Filter on OWNER_DEPARTMENT using bind :dept.",
    "entity_no": "Filter on ENTITY_NO using bind :entity_no.",
    "contract_id_pattern": "Filter on CONTRACT_ID (LIKE) using bind :contract_id_pattern.",
    "request_type": "Filter on REQUEST_TYPE using bind :request_type.",
}


def _context_notes(context: Optional[Dict[str, Any]]) -> List[str]:
    if not context:
        return []
    notes: List[str] = []
    window = context.get("date_window") if isinstance(context, dict) else None
    date_column = context.get("date_column") if isinstance(context, dict) else None
    if window:
        column = date_column or "REQUEST_DATE"
        label = context.get("window_label") or "requested window"
        notes.append(
            f"Apply the {label} on column {column} using binds :date_start and :date_end. Do not inline literal dates."
        )
    hints = context.get("hints") if isinstance(context, dict) else None
    if hints and "stakeholder_unpivot" in hints:
        notes.append(
            "If aggregating by stakeholder, union the slots (CONTRACT_STAKEHOLDER_1..8 with DEPARTMENT_1..8) into rows"
            " with columns CONTRACT_ID, STAKEHOLDER, DEPARTMENT, REQUEST_DATE AS REF_DATE, and compute gross as"
            " NVL(CONTRACT_VALUE_NET_OF_VAT,0)+NVL(VAT,0)."
        )
    top_n = context.get("top_n") if isinstance(context, dict) else None
    if top_n:
        notes.append(
            "For TOP queries, order appropriately and apply FETCH FIRST :top_n ROWS ONLY (defaults to 10 if unspecified)."
        )
    filters = context.get("filters") if isinstance(context, dict) else None
    if isinstance(filters, dict):
        for key, instruction in _FILTER_INSTRUCTIONS.items():
            if key in filters:
                notes.append(instruction)
    return notes


def build_prompt(question: str, context: Optional[Dict[str, Any]] = None) -> str:
    question = (question or "").strip()
    notes = _context_notes(context)
    if notes:
        context_block = "\n\nContext:\n" + "\n".join(f"- {note}" for note in notes)
    else:
        context_block = ""
    return PROMPT.format(question=f"{question}{context_block}", begin=_BEGIN, end=_END)


def extract_sql(text: str) -> Optional[str]:
    if not text:
        return None
    match = re.search(rf"{_BEGIN}\s*(.*?)\s*{_END}", text, flags=re.S | re.I)
    if match:
        sql = match.group(1).strip()
    else:
        fallback = re.search(r"(?is)\b(?:select|with)\b.*", text)
        sql = fallback.group(0).strip() if fallback else None
    if not sql:
        return None
    if not re.match(r"(?is)^(select|with)\b", sql):
        return None
    return sql


def _sanitize_oracle_select(raw: str) -> Optional[str]:
    if not raw:
        return None
    lines = raw.splitlines()
    start = None
    for idx, line in enumerate(lines):
        stripped = line.strip().lower()
        if stripped.startswith("select") or stripped.startswith("with"):
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


def nl_to_sql_with_llm(question: str, context: Optional[Dict[str, Any]] = None) -> Optional[str]:
    model = get_model("sql")
    prompt = build_prompt(question, context=context)
    raw = model.generate(prompt, max_new_tokens=320, stop=[])
    sql = extract_sql(raw)
    sql = _sanitize_oracle_select(sql) if sql else None
    if sql:
        return sql

    repair_prompt = "Return only Oracle SELECT between BEGIN_SQL and END_SQL.\n\n" + prompt
    raw_retry = model.generate(repair_prompt, max_new_tokens=320, stop=[])
    sql_retry = extract_sql(raw_retry)
    sql_retry = _sanitize_oracle_select(sql_retry) if sql_retry else None
    return sql_retry


__all__ = [
    "PROMPT",
    "build_prompt",
    "extract_sql",
    "nl_to_sql_with_llm",
]
