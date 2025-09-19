import re
from typing import Dict, List, Tuple

from core.model_loader import get_model

_ALLOWED_BINDS = {
    "date_start",
    "date_end",
    "top_n",
    "owner_name",
    "dept",
    "entity_no",
    "contract_id_pattern",
    "request_type",
}

_SYSTEM_TMPL = """You are an Oracle SQL generator.
Return ONLY a single Oracle SELECT or WITH query. No prose. No comments. No 'SQL:' prefix.
Use only table "Contract".
Use only these columns: {cols}.
Do NOT add a date filter unless the user explicitly asks (e.g., 'next 30 days', 'last month', 'between', 'since').
If you use binds, they MUST be from this whitelist only: :date_start, :date_end, :top_n, :owner_name, :dept, :entity_no, :contract_id_pattern, :request_type.
Never bind obvious literals like 0, 1, 'ACTIVE'—write them as literals.
Use Oracle syntax: NVL(), TRIM(), UPPER(), LISTAGG(... WITHIN GROUP (...)), FETCH FIRST N ROWS ONLY.
"""


def _extract_sql(text: str) -> str:
    """Return the first SELECT/WITH block, strip comments/fences; empty if not found."""

    if not text:
        return ""

    cleaned = re.sub(r"```sql|```", "", text, flags=re.IGNORECASE)
    match = re.search(r"\b(SELECT|WITH)\b", cleaned, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        return ""

    sql = cleaned[match.start() :].strip()
    lines = [line for line in sql.splitlines() if not line.strip().startswith("--")]
    sql = "\n".join(lines).strip()
    if sql.endswith(";"):
        sql = sql[:-1].strip()
    return sql


def nl_to_sql_with_llm(question: str, allowed_columns: List[str]) -> Tuple[str, Dict]:
    """Generate Oracle SQL from NL, then extract/clean it."""

    mdl = get_model("sql")
    sys_prompt = _SYSTEM_TMPL.format(cols=", ".join(allowed_columns))
    prompt = f"{sys_prompt}\n\nQuestion:\n{question}\nSQL:"
    raw = mdl.generate(prompt, stop=["</s>", "<|im_end|>"])
    sql = _extract_sql(raw)
    return sql, {"raw": raw}


__all__ = ["_ALLOWED_BINDS", "_SYSTEM_TMPL", "_extract_sql", "nl_to_sql_with_llm"]
