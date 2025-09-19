"""DocuWare SQL generation helper using the shared SQLCoder model."""

from __future__ import annotations

import re
from typing import Dict

from core.model_loader import load_llm


_CODE_BLOCK = re.compile(r"```(?:sql)?\s*(.*?)```", re.IGNORECASE | re.DOTALL)


def nl_to_sql_with_llm(question: str, dw_table: str = "Contract") -> Dict[str, object]:
    """Use SQLCoder to translate natural language into Oracle SQL."""

    llm = load_llm("sql")
    if not llm:
        return {"sql": None, "confidence": 0.0, "why": "sql_model_unavailable"}

    generator = llm.get("handle")
    if generator is None:
        return {"sql": None, "confidence": 0.0, "why": "sql_generator_missing"}

    sys_prompt = (
        "You convert natural language to **Oracle SQL** over a single table named {tbl}.\n"
        "Rules:\n"
        "- Output only SQL (no comments), SELECT/CTEs only (no DML/DDL).\n"
        "- Use NVL for null-safe arithmetic.\n"
        "- Gross value = NVL(CONTRACT_VALUE_NET_OF_VAT,0) + NVL(VAT,0).\n"
        "- If time window like 'last month/90 days/next 30 days' is present, use named binds :date_start and :date_end.\n"
        "- Use LISTAGG for aggregating text when asked.\n"
    ).format(tbl=dw_table)

    prompt = f"{sys_prompt}\nQuestion: {question}\nGenerate SQL now.\nSQL:"

    try:
        raw_text = generator.generate(prompt)
    except Exception as exc:
        return {"sql": None, "confidence": 0.0, "why": f"generator_error: {exc}"}

    candidate = (raw_text or "").strip()
    match = _CODE_BLOCK.search(candidate)
    if match:
        candidate = match.group(1).strip()
    candidate = candidate.rstrip(";")
    if not candidate.lower().startswith(("with", "select")):
        return {"sql": None, "confidence": 0.2, "why": "not_select"}

    lowered = candidate.lower()
    if any(word in lowered for word in (" delete ", " update ", " insert ", " drop ", " alter ", " truncate ")):
        return {"sql": None, "confidence": 0.0, "why": "unsafe_sql"}

    return {"sql": candidate, "confidence": 0.72, "why": "ok"}
