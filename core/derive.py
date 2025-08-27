from __future__ import annotations
from typing import Any, Dict, Iterable, Tuple

def derive_sql_from_admin_reply(
    llm: Any,
    *,
    question: str,
    admin_reply: str,
    tables: Iterable[str],
    columns: Iterable[str],
    metrics: Iterable[str] | None = None,
) -> Tuple[str | None, str | None]:
    """
    Try to convert admin natural-language reply into canonical SQL (no prefixes).
    Returns (sql, rationale) or (None, None) if not confident.
    """
    tbls = ", ".join(sorted(set(tables)))
    cols = ", ".join(sorted(set(columns)))
    mets = ", ".join(sorted(set(metrics or []))) or "(none)"
    prompt = (
        "You are an expert SQL planner. Convert the admin guidance into canonical SQL.\n"
        "Use only these tables/columns.\n"
        f"Tables: {tbls}\n"
        f"Columns: {cols}\n"
        f"Metrics: {mets}\n"
        f"Original Question: {question}\n"
        f"Admin Guidance: {admin_reply}\n"
        "Return as:\nSQL:\n<sql>\nRationale:\n<why>\n"
        "If you cannot produce safe SELECT/CTE SQL, return literally: SQL:\n<none>\nRationale:\n<why>\n"
    )
    out = llm.generate(prompt, max_new_tokens=256, temperature=0.1, top_p=0.9)
    lower = out.lower()
    if "sql:" in lower:
        i = lower.find("sql:")
        rest = out[i+4:]
        j = rest.lower().find("rationale:")
        sql = rest[:j].strip() if j >= 0 else rest.strip()
        why = rest[j+10:].strip() if j >= 0 else ""
        if "<none>" in sql.lower():
            return None, why
        return sql, why
    return None, None
