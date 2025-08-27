# core/inquiries.py
"""
Inquiry helpers: create/update/list feedback entries in mem_inquiries.

These helpers are project-agnostic (no FA-specific code).
"""

from __future__ import annotations


from typing import Any, Dict, List, Optional, Tuple
from sqlalchemy.engine import Engine
from sqlalchemy import text, bindparam
from sqlalchemy.dialects.postgresql import JSONB


def mark_admin_note(mem_engine, *, inquiry_id: int, admin_reply: str, answered_by: str) -> None:
    with mem_engine.begin() as con:
        con.execute(text("""
            UPDATE mem_inquiries
               SET admin_reply = :reply,
                   answered_by = :by,
                   updated_at = NOW()
             WHERE id = :id
        """), {"reply": admin_reply, "by": answered_by, "id": inquiry_id})

def create_or_update_inquiry(
    mem_engine: Engine,
    *,
    namespace: str,
    prefixes: List[str],
    question: str,
    auth_email: Optional[str],
    run_id: Optional[int],
    research_enabled: bool,
    status: str = "open",
    research_summary: Optional[str] = None,
    source_ids: Optional[List[int]] = None,
) -> int:
    """
    Upsert/insert a new inquiry row for auditing & follow-up.
    Returns the inquiry id.

    Parameters
    ----------
    namespace: active memory namespace, e.g. "fa::579_"
    prefixes: FA prefixes used for this inquiry (array)
    question: user question text
    auth_email: requester email to receive exports
    run_id: optional mem_runs.id if already planned/executed
    research_enabled: True if RESEARCH_MODE was on for this request
    status: open | awaiting_admin | answered | needs_clarification | needs_fix
    research_summary: optional text summary from web research
    source_ids: list of mem_sources ids that informed the answer
    """

    sql = text("""
            INSERT INTO mem_inquiries(
                namespace, prefixes, question, auth_email,
                run_id, research_enabled, research_summary, source_ids, status,
                created_at, updated_at
            )
            VALUES (
                :ns, :pfx, :q, :mail,
                :run_id, :re, :rs, :src, :st,
                NOW(), NOW()
            )
            RETURNING id
        """).bindparams(
        bindparam("pfx", type_=JSONB),
        bindparam("src", type_=JSONB),
    )

    params = {
        "ns": namespace,
        "pfx": prefixes or [],  # <- python list, not JSON string
        "q": question,
        "mail": auth_email,
        "run_id": run_id,
        "re": bool(research_enabled),
        "rs": research_summary,
        "src": source_ids or [],  # <- python list, not JSON string
        "st": status,
    }

    with mem_engine.begin() as con:
        new_id = con.execute(sql, params).scalar_one()
    return int(new_id)




def set_feedback(mem_engine: Engine, *, inquiry_id: int, satisfied: bool, rating: Optional[int], comment: Optional[str]) -> None:
    """Stores user feedback (satisfied / rating / comment)."""
    with mem_engine.begin() as con:
        con.execute(text("""
            UPDATE mem_inquiries
               SET satisfied=:sat, rating=:rate, feedback_comment=:c, updated_at=NOW()
             WHERE id=:id
        """), {"id": inquiry_id, "sat": satisfied, "rate": rating, "c": comment})

def list_inquiries(mem_engine: Engine, *, namespace: str, status: Optional[str], limit: int = 50) -> List[Dict[str, Any]]:
    """List inquiries by namespace and optional status."""
    q = """
      SELECT id, question, auth_email, status, created_at, updated_at
      FROM mem_inquiries WHERE namespace=:ns
    """
    if status:
        q += " AND status=:st"
    q += " ORDER BY created_at DESC LIMIT :lim"
    params = {"ns": namespace, "st": status, "lim": limit}
    with mem_engine.connect() as con:
        rows = con.execute(text(q), params).mappings().all()
        return [dict(r) for r in rows]

def mark_answered(mem_engine: Engine, *, inquiry_id: int, answered_by: str, admin_reply: Optional[str]) -> None:
    """Mark inquiry as answered and store admin reply."""
    with mem_engine.begin() as con:
        con.execute(text("""
            UPDATE mem_inquiries
               SET status='answered', answered_by=:by, answered_at=NOW(),
                   admin_reply=:rep, updated_at=NOW()
             WHERE id=:id
        """), {"id": inquiry_id, "by": answered_by, "rep": admin_reply})

