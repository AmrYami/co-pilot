# core/admin_helpers.py
from __future__ import annotations
from typing import Any, Dict, Optional, Tuple

from core.pipeline import Pipeline

def derive_sql_from_admin_reply(
    pipeline: Pipeline,
    inq: Dict[str, Any],
    admin_reply: str,
    *,
    source: str = "fa",
) -> Tuple[Optional[str], Dict[str, Any]]:
    """
    Derive runnable SQL from an admin's natural-language reply for a specific inquiry.

    What this does (high level):
      1) Build an augmented question that appends the admin's hints to the original user question.
      2) Ask the pipeline planner to produce canonical SQL (unprefixed) using the normal context builder.
      3) Let the pipeline rewrite canonical SQL to tenant-prefixed SQL (based on inquiry prefixes).
      4) Validate the SQL using the pipeline's validator (EXPLAIN-only safety).
      5) Return (sql, meta) on success; otherwise (None, info) with status or error details.

    Parameters
    ----------
    pipeline : Pipeline
        The live pipeline instance (already holds settings, engines, LLM, validator, etc.).
    inq : dict
        A row-like mapping for the inquiry (must contain 'question' and optionally 'prefixes').
    admin_reply : str
        Admin free-text guidance (no SQL required).
    source : str
        Logical source handled by the pipeline. Defaults to "fa" but kept generic.

    Returns
    -------
    Tuple[Optional[str], Dict[str, Any]]
        On success: (sql_string, {"status":"ok", "context":..., "rationale":...})
        On failure: (None, {"status": "...", "questions":[...], "context":...}) OR
                    (None, {"error": "...", "details": ...})
    """
    # ---- Step 0: input guards (small note: we fail fast if the inquiry is malformed)
    prefixes = inq.get("prefixes") or []
    question = (inq.get("question") or "").strip()
    if not question:
        return None, {"error": "inquiry has no question"}

    # ---- Step 1: augment question with admin hints (small note: keeps planner prompt simple)
    augmented_q = f"{question}\n\nADMIN HINTS: {admin_reply}".strip()

    # ---- Step 2: plan via pipeline (small note: returns canonical SQL + rationale)
    plan_out = pipeline.answer(source=source, prefixes=prefixes, question=augmented_q)

    # If planner still needs clarification or failed, bubble that up unchanged
    if plan_out.get("status") != "ok":
        return None, {
            "status": plan_out.get("status"),
            "questions": plan_out.get("questions"),
            "context": plan_out.get("context"),
            "rationale": plan_out.get("rationale"),
        }

    sql = (plan_out.get("sql") or "").strip()
    if not sql:
        return None, {"error": "planner returned empty SQL", "context": plan_out.get("context")}

    # ---- Step 3/4: validate via pipeline’s validator (small note: EXPLAIN probe)
    ok, info = pipeline.validator.quick_validate(sql)
    if not ok:
        return None, {"error": "validation failed", "details": info, "context": plan_out.get("context")}

    # ---- Step 5: done (small note: return rationale + context to help the UI)
    return sql, {"status": "ok", "context": plan_out.get("context"), "rationale": plan_out.get("rationale")}
