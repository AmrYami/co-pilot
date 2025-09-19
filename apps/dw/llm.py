"""DocuWare SQL generation helper using the shared SQLCoder model."""

from __future__ import annotations

import json
import re
from typing import Any, Dict, Optional

from core.model_loader import get_model, load_llm


_CODE_BLOCK = re.compile(r"```(?:sql)?\s*(.*?)```", re.IGNORECASE | re.DOTALL)

CLARIFIER_SYSTEM = """You are a careful analyst. 
Return a single compact JSON object describing the user's requested intent over the DocuWare `Contract` table.
Schema highlights:
- Monetary: CONTRACT_VALUE_NET_OF_VAT (number), VAT (number), GROSS = NVL(CONTRACT_VALUE_NET_OF_VAT,0)+NVL(VAT,0)
- Dates: REQUEST_DATE, START_DATE, END_DATE, EXPIERY_30/60/90
- Stakeholders/Departments: CONTRACT_STAKEHOLDER_1..8 paired with DEPARTMENT_1..8
- Owner department: OWNER_DEPARTMENT, and DEPARTMENT_OUL is the org lead

JSON fields to output (only these):
{
  "intent": "select" | "rank" | "count" | "sum" | "avg",
  "entity": "contracts" | "stakeholders" | "departments",
  "time": {"column": "REQUEST_DATE"|"START_DATE"|"END_DATE", "range": {"type": "last_month"|"last_90_days"|"...", "start": "YYYY-MM-DD", "end": "YYYY-MM-DD"}},
  "group_by": ["stakeholder"|"department"|"owner_department"|"..."],
  "metrics": ["gross_value","count_contracts", "..."],
  "top_n": 10
}
If unclear, infer sensible defaults (REQUEST_DATE for generic “last month”, stakeholder view for stakeholder questions).
Return ONLY the JSON, no prose.
"""


def clarify_intent(question: str, context: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    mdl = get_model("clarifier")
    if not mdl:
        return None
    user = f"Question: {question}\nContext: {json.dumps(context, ensure_ascii=False)}"
    try:
        out = mdl.generate(
            system_prompt=CLARIFIER_SYSTEM,
            user_prompt=user,
            max_new_tokens=256,
        )
    except Exception:
        return None
    if not out:
        return None
    try:
        start = out.find("{")
        end = out.rfind("}")
        if start != -1 and end != -1 and end > start:
            return json.loads(out[start : end + 1])
    except Exception:
        return None
    return None


def nl_to_sql_with_llm(
    question: Optional[str] = None,
    *,
    intent: Optional[Dict[str, Any]] = None,
    settings: Optional[Any] = None,
    dw_table: str = "Contract",
) -> Dict[str, object]:
    """Use SQLCoder to translate natural language or structured intent into Oracle SQL."""

    llm = load_llm("sql")
    if not llm:
        return {"sql": None, "confidence": 0.0, "why": "sql_model_unavailable"}

    generator = llm.get("handle")
    if generator is None:
        return {"sql": None, "confidence": 0.0, "why": "sql_generator_missing"}

    if not question and not intent:
        return {"sql": None, "confidence": 0.0, "why": "missing_prompt"}

    # Baseline system instructions for SQLCoder in DocuWare/Oracle environment.
    sys_prompt = (
        f"You are SQLCoder generating Oracle SQL for the DocuWare `{dw_table}` table.\n"
        "Requirements:\n"
        "- Output SELECT statements only (CTEs allowed), never DML/DDL.\n"
        "- Oracle dialect: use NVL, FETCH FIRST :top_n ROWS ONLY, LISTAGG ... WITHIN GROUP.\n"
        "- Available tables: Contract.\n"
        "- Gross value = NVL(CONTRACT_VALUE_NET_OF_VAT,0) + NVL(VAT,0).\n"
        "- Time filters must bind :date_start and :date_end.\n"
        "- Stakeholder/department slots 1..8 require UNION ALL across slots with matching departments.\n"
        "- Never create or reference views.\n"
        "Return only SQL (no commentary)."
    )

    prompt_parts = [sys_prompt]

    context_hints: Dict[str, Any] = {}
    if settings is not None:
        try:
            context_hints = settings.get("DW_PROMPT_HINTS", scope="namespace") or {}
        except Exception:
            context_hints = {}
    if context_hints:
        hint_text = "Additional context hints:\n" + json.dumps(
            context_hints, ensure_ascii=False, indent=2
        )
        prompt_parts.append(hint_text)
    if intent:
        intent_json = json.dumps(intent, ensure_ascii=False, indent=2)
        prompt_parts.append(f"Structured intent JSON:\n```json\n{intent_json}\n```")
    if question:
        prompt_parts.append(f"Question: {question}")

    prompt = "\n\n".join(prompt_parts) + "\nGenerate Oracle SQL now.\nSQL:"

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

    confidence = 0.72
    if intent:
        confidence = 0.82
    return {"sql": candidate, "confidence": confidence, "why": "ok"}
