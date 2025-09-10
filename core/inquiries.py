# core/inquiries.py
"""
Inquiry helpers: create/update/list feedback entries in mem_inquiries.

These helpers are project-agnostic (no FA-specific code).
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from sqlalchemy.engine import Engine
from sqlalchemy import text, bindparam

try:
    # Optional: only present on Postgres
    from sqlalchemy.dialects.postgresql import JSONB
except Exception:  # pragma: no cover
    JSONB = None  # type: ignore


def mark_admin_note(mem_engine, *, inquiry_id: int, admin_reply: str, answered_by: str) -> None:
    with mem_engine.begin() as con:
        con.execute(text("""
            UPDATE mem_inquiries
               SET admin_reply = :reply,
                   answered_by = :by,
                   updated_at = NOW()
             WHERE id = :id
        """), {"reply": admin_reply, "by": answered_by, "id": inquiry_id})


def append_admin_note(mem_engine, inquiry_id: int, by: str, text_note: str) -> None:
    note = {
        "by": by,
        "text": text_note,
        "ts": datetime.now(timezone.utc).isoformat(),
    }
    with mem_engine.begin() as c:
        r = (
            c.execute(
                text("SELECT admin_notes FROM mem_inquiries WHERE id=:id FOR UPDATE"),
                {"id": inquiry_id},
            )
            .mappings()
            .first()
        )
        notes = r["admin_notes"] or []
        if isinstance(notes, str):
            try:
                notes = json.loads(notes) or []
            except Exception:
                notes = []
        if not isinstance(notes, list):
            notes = []
        notes.append(note)
        c.execute(
            text(
                """UPDATE mem_inquiries
                    SET admin_notes=:notes,
                        clarification_rounds = COALESCE(clarification_rounds,0) + 1,
                        updated_at = NOW()
                    WHERE id=:id"""
            ),
            {"id": inquiry_id, "notes": json.dumps(notes)},
        )

def get_inquiry(conn, inquiry_id: int) -> dict | None:
    row = conn.execute(
        text(
            """SELECT id, namespace, prefixes, question, auth_email,
                       status, admin_notes, clarification_rounds, last_question
                FROM mem_inquiries
                WHERE id = :id"""
        ),
        {"id": inquiry_id},
    ).mappings().first()
    return dict(row) if row else None


def set_inquiry_status(
    mem_engine,
    inquiry_id: int,
    status: str,
    last_question: str | None = None,
    answered_by: str | None = None,
) -> None:
    with mem_engine.begin() as c:
        c.execute(
            text(
                """UPDATE mem_inquiries
                    SET status=:st,
                        last_question = :lq,
                        answered_by = COALESCE(:by, answered_by),
                        updated_at = NOW()
                    WHERE id=:id"""
            ),
            {"id": inquiry_id, "st": status, "lq": last_question, "by": answered_by},
        )


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
    Insert a new inquiry row. Tries full JSONB/modern schema first,
    then falls back to a minimal, legacy-compatible insert if needed.
    Returns the new inquiry id.
    """
    pfx_list = prefixes or []
    src_list = source_ids or []

    # Try modern schema (JSONB + extra columns)
    try:
        params: Dict[str, Any] = {
            "ns": namespace,
            "pfx": pfx_list,
            "q": question,
            "mail": auth_email,
            "run_id": run_id,
            "re": bool(research_enabled),
            "rs": research_summary,
            "src": src_list,
            "st": status,
        }
        sql = text("""
            INSERT INTO mem_inquiries(
                namespace, prefixes, question, auth_email,
                run_id, research_enabled, research_summary, source_ids,
                status, created_at, updated_at
            )
            VALUES (
                :ns, :pfx, :q, :mail,
                :run_id, :re, :rs, :src,
                :st, NOW(), NOW()
            )
            RETURNING id
        """)
        if JSONB is not None:
            sql = sql.bindparams(
                bindparam("pfx", type_=JSONB),
                bindparam("src", type_=JSONB),
            )

        with mem_engine.begin() as con:
            new_id = con.execute(sql, params).scalar_one()
            return int(new_id)

    except Exception:
        # Fall back to a very small subset that matches legacy tables.
        # Store prefixes/source_ids as TEXT (JSON string) if needed.
        params2: Dict[str, Any] = {
            "ns": namespace,
            "pfx_txt": json.dumps(pfx_list, ensure_ascii=False),
            "q": question,
            "mail": auth_email,
            "st": status,
        }
        sql2 = text("""
            INSERT INTO mem_inquiries(
                namespace, prefixes, question, auth_email,
                status, created_at, updated_at
            )
            VALUES (
                :ns, :pfx_txt, :q, :mail,
                :st, NOW(), NOW()
            )
            RETURNING id
        """)
        with mem_engine.begin() as con:
            new_id = con.execute(sql2, params2).scalar_one()
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
