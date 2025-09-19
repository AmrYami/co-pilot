# apps/dw/llm.py
import re
from typing import Optional, Tuple

from core.model_loader import llm_complete, get_model

# Allowed columns for Contract (kept tight so the model stays on the rails)
_ALLOWED_COLS = [
    "CONTRACT_ID","CONTRACT_OWNER",
    "CONTRACT_STAKEHOLDER_1","CONTRACT_STAKEHOLDER_2","CONTRACT_STAKEHOLDER_3","CONTRACT_STAKEHOLDER_4",
    "CONTRACT_STAKEHOLDER_5","CONTRACT_STAKEHOLDER_6","CONTRACT_STAKEHOLDER_7","CONTRACT_STAKEHOLDER_8",
    "DEPARTMENT_1","DEPARTMENT_2","DEPARTMENT_3","DEPARTMENT_4","DEPARTMENT_5","DEPARTMENT_6","DEPARTMENT_7","DEPARTMENT_8",
    "OWNER_DEPARTMENT",
    "CONTRACT_VALUE_NET_OF_VAT","VAT",
    "CONTRACT_PURPOSE","CONTRACT_SUBJECT",
    "START_DATE","END_DATE","REQUEST_DATE","REQUEST_TYPE","CONTRACT_STATUS","ENTITY_NO","REQUESTER"
]

def _strip_fences_and_comments(txt: str) -> str:
    # remove code fences
    txt = re.sub(r"^```[\s\S]*?\n", "", txt.strip())
    txt = re.sub(r"```$", "", txt.strip())
    # drop leading SQL comments (lines starting with -- ) until first SELECT/WITH
    lines = txt.splitlines()
    kept = []
    found = False
    for ln in lines:
        if re.search(r"\b(SELECT|WITH)\b", ln, flags=re.I):
            found = True
        if found:
            kept.append(ln)
    cleaned = "\n".join(kept).strip() if found else txt.strip()
    # keep only the first statement (avoid accidental semicolons)
    cleaned = cleaned.split(";")[0].strip()
    return cleaned

def _first_select_or_with(txt: str) -> Optional[str]:
    m = re.search(r"(?is)\b(SELECT|WITH)\b[\s\S]*", txt)
    return m.group(0).strip() if m else None

def _oracle_select_only(sql: str) -> Optional[str]:
    if not sql:
        return None
    sql = _strip_fences_and_comments(sql)
    frag = _first_select_or_with(sql)
    if not frag:
        return None
    # forbid DML/DDL just in case
    if re.search(r"(?i)\b(UPDATE|DELETE|INSERT|MERGE|DROP|ALTER|TRUNCATE|CREATE)\b", frag):
        return None
    return frag

def _sqlcoder_prompt(question: str) -> str:
    allowed = ", ".join(_ALLOWED_COLS)
    return (
        "Return ONLY Oracle SQL. No prose. No comments. SELECT/CTE only.\n"
        'Table: "Contract"\n'
        f"Allowed columns: {allowed}\n"
        "Rules:\n"
        " - Use Oracle syntax: NVL, LISTAGG ... WITHIN GROUP, TRIM, UPPER, FETCH FIRST N ROWS ONLY.\n"
        " - If a time window is explicitly requested (e.g., next 30 days, last month), "
        "   filter on the referenced date column using :date_start and :date_end binds.\n"
        f" - If the user names END_DATE, use END_DATE (not REQUEST_DATE) for any window.\n"
        " - Never write DML/DDL. SELECT/CTE only.\n"
        "\n"
        "Question:\n"
        f"{question}\n"
        "SQL:\n"
    )

def nl_to_sql_with_llm(question: str, max_new_tokens: int = 256) -> Tuple[Optional[str], str]:
    """Generate SQL from natural language using the SQL model, clean it, and return (sql, raw_out)."""
    prompt = _sqlcoder_prompt(question)
    raw = llm_complete(
        role="sql",
        prompt=prompt,
        max_new_tokens=max_new_tokens,
        temperature=0.2,
        top_p=0.9,
        stop=None,
    )
    sql = _oracle_select_only(raw)
    return sql, raw

def _clarifier_prompt(question: str) -> str:
    return (
        "Rewrite the user's question into a single, precise SQL-style instruction, without changing its meaning.\n"
        "Keep it short (<30 tokens), and explicitly name a date column only if the user asked for a time window.\n"
        f"User: {question}\n"
        "Rewrite:\n"
    )

def clarify_for_sql(question: str, max_new_tokens: int = 96) -> str:
    # If clarifier model is disabled, just return the original question
    try:
        get_model("clarifier")
    except Exception:
        return question

    out = llm_complete(
        role="clarifier",
        prompt=_clarifier_prompt(question),
        max_new_tokens=max_new_tokens,
        temperature=0.0,
        stop=None,
    )
    # keep it one line and short
    out = out.strip().splitlines()[0]
    return out or question
