# core/admin_helpers.py
from __future__ import annotations
from typing import Any, Dict, Optional, Tuple
from sqlalchemy.engine import Engine
from sqlalchemy import text

from core.pipeline import Pipeline
from core.agents import ValidatorAgent

def derive_sql_from_admin_reply(
    pipeline: Pipeline,
    inq: Dict[str, Any],
    admin_reply: str
) -> Tuple[Optional[str], Dict[str, Any]]:
    """
    Try to produce runnable SQL from the original question + admin's reply.
    Returns (sql or None, info dict with rationale/context/status).
    Strategy:
      1) Treat admin_reply as extra intent: "Original: <q>. Admin hints: <reply>"
      2) Ask pipeline Planner to generate canonical SQL, then rewrite to prefixes.
      3) Validate with EXPLAIN.
    """
    prefixes = inq.get("prefixes") or []
    question = (inq.get("question") or "").strip()
    if not question:
        return None, {"error": "inquiry has no question"}

    # Build a composite hint for the planner
    augmented_q = f"{question}\n\nADMIN HINTS: {admin_reply}".strip()

    # Ask the pipeline to plan with the usual FA context
    plan_out = pipeline.answer(source="fa", prefixes=prefixes, question=augmented_q)

    if plan_out.get("status") != "ok":
        # Could be needs_clarification/needs_fix, bubble it up so caller decides
        return None, {"status": plan_out.get("status"), "questions": plan_out.get("questions"), "context": plan_out.get("context")}

    sql = plan_out.get("sql")
    if not sql:
        return None, {"error": "planner returned empty SQL"}

    # Validate with EXPLAIN
    val = ValidatorAgent(pipeline.fa_engine)
    ok, info = val.quick_validate(sql)
    if not ok:
        return None, {"error": "validation failed", "details": info, "context": plan_out.get("context")}

    return sql, {"status": "ok", "context": plan_out.get("context"), "rationale": plan_out.get("rationale")}
