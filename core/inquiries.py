# core/inquiries.py
"""
Inquiry helpers: create/update/list feedback entries in mem_inquiries.

These helpers are project-agnostic (no FA-specific code).
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from sqlalchemy.engine import Engine, Row
from sqlalchemy import text, bindparam
from sqlalchemy.dialects.postgresql import JSONB


def fetch_inquiry(mem_engine, inquiry_id: int) -> Optional[Dict[str, Any]]:
    with mem_engine.begin() as c:
        row = c.execute(
            text(
                """
            SELECT id, namespace, prefixes, question, auth_email,
                   status, clarification_rounds, admin_notes, run_id,
                   last_sql, last_error, result_sample, created_at, updated_at
              FROM mem_inquiries
             WHERE id = :id
            """
            ),
            {"id": inquiry_id},
        ).mappings().first()
        return dict(row) if row else None


def get_admin_notes(inquiry_row: Dict[str, Any]) -> List[str]:
    notes = inquiry_row.get("admin_notes") or []
    out: List[str] = []
    for n in notes:
        txt = n.get("text") if isinstance(n, dict) else None
        if txt:
            out.append(str(txt))
    return out


def summarize_admin_notes(notes: Optional[List[Dict[str, Any]]]) -> str:
    """Join all admin note texts into a single context string for the planner."""
    if not notes:
        return ""
    lines = []
    for n in notes:
        t = (n or {}).get("text")
        if t:
            lines.append(str(t).strip())
    return "\n".join(lines).strip()


# --- Admin note helpers ----------------------------------------------------


def append_admin_note(mem_engine, inquiry_id: int, *, by: str, text_note: str) -> int:
    note = {
        "by": by,
        "text": (text_note or "").strip(),
        "ts": datetime.now(timezone.utc).isoformat(),
    }
    note_json = json.dumps(note)

    sql = text(
        """
        UPDATE mem_inquiries
           SET admin_notes          = COALESCE(admin_notes, '[]'::jsonb)
                                      || jsonb_build_array(to_jsonb(%(note)s::json)),
               admin_reply          = COALESCE(%(reply)s, admin_reply),
               answered_by          = COALESCE(%(by)s, answered_by),
               clarification_rounds = COALESCE(clarification_rounds, 0) + 1,
               updated_at           = NOW()
         WHERE id = %(id)s
     RETURNING clarification_rounds
        """
    )
    with mem_engine.begin() as c:
        r = c.execute(
            sql,
            {"id": inquiry_id, "by": by, "reply": text_note, "note": note_json},
        )
        rounds = r.scalar_one()
    return rounds


def set_admin_reply(mem_engine, inquiry_id: int, reply: str, answered_by: str | None = None) -> None:
    sql = text(
        """
        UPDATE mem_inquiries
           SET admin_reply = :reply,
               answered_by = COALESCE(:by, answered_by),
               updated_at  = NOW()
         WHERE id = :id
    """
    )
    with mem_engine.begin() as c:
        c.execute(sql, {"id": inquiry_id, "reply": reply, "by": answered_by})


def update_inquiry_status_run(mem_engine, inquiry_id: int, *,
                              status: str,
                              run_id: Optional[int] = None,
                              answered_by: Optional[str] = None,
                              answered_at: Optional[Any] = None) -> None:
    with mem_engine.begin() as c:
        c.execute(text(
            """
            UPDATE mem_inquiries
               SET status = :st,
                   run_id = COALESCE(:rid, run_id),
                   answered_by = COALESCE(:ab, answered_by),
                   answered_at = COALESCE(:aat, answered_at),
                   updated_at = NOW()
             WHERE id = :id
            """
        ), {
            "id": inquiry_id,
            "st": status,
            "rid": run_id,
            "ab": answered_by,
            "aat": answered_at
        })


def get_inquiry_row(mem: Engine, inquiry_id: int) -> Optional[Row]:
    sql = text(
        """
        SELECT *
        FROM mem_inquiries
        WHERE id = :id
        LIMIT 1
    """
    )
    with mem.begin() as c:
        r = c.execute(sql, {"id": inquiry_id}).fetchone()
    return r


def get_inquiry_details(mem: Engine, inquiry_id: int) -> Dict[str, Any]:
    """
    Aggregate an inquiry, last run, last error, and sample.
    """
    base = get_inquiry_row(mem, inquiry_id)
    if not base:
        return {"ok": False, "error": "not_found", "id": inquiry_id}

    run_sql = text(
        """
        WITH pick AS (
            SELECT COALESCE(mi.run_id,
                            (SELECT id FROM mem_runs
                             WHERE namespace = mi.namespace
                               AND input_query = mi.question
                             ORDER BY created_at DESC
                             LIMIT 1)) AS rid
            FROM mem_inquiries mi
            WHERE mi.id = :id
        )
        SELECT r.*
        FROM pick p
        LEFT JOIN mem_runs r ON r.id = p.rid
    """
    )
    err_sql = text(
        """
        SELECT e.*
        FROM mem_errors e
        WHERE e.run_id = :run_id
        ORDER BY e.created_at DESC
        LIMIT 1
    """
    )
    with mem.begin() as c:
        run = c.execute(run_sql, {"id": inquiry_id}).fetchone()
        last_error = None
        if run and run.id:
            last_error = c.execute(err_sql, {"run_id": run.id}).fetchone()

    out = {
        "ok": True,
        "id": base.id,
        "namespace": base.namespace,
        "status": base.status,
        "run_id": getattr(base, "run_id", None),
        "auth_email": getattr(base, "auth_email", None),
        "clarification_rounds": getattr(base, "clarification_rounds", 0),
        "admin_notes": getattr(base, "admin_notes", []),
        "question": base.question,
        "prefixes": base.prefixes,
        "created_at": base.created_at,
        "updated_at": base.updated_at,
        "last_sql": None,
        "last_error": None,
        "sample": None,
    }
    if run:
        out["run"] = {
            "id": run.id,
            "status": run.status,
            "sql_generated": run.sql_generated,
            "sql_final": run.sql_final,
            "rows_returned": run.rows_returned,
            "execution_time_ms": run.execution_time_ms,
            "error_message": run.error_message,
            "result_sample": run.result_sample,
            "created_at": run.created_at,
        }
        out["last_sql"] = run.sql_final or run.sql_generated
        out["sample"] = run.result_sample
        out["last_error"] = (
            last_error.error_message if last_error else (run.error_message or None)
        )

    return out


def mark_admin_note(mem_engine, *, inquiry_id: int, admin_reply: str, answered_by: str) -> None:
    with mem_engine.begin() as con:
        con.execute(
            text(
                """
            UPDATE mem_inquiries
               SET admin_reply = :reply,
                   answered_by = :by,
                   updated_at = NOW()
             WHERE id = :id
        """
            ),
            {"reply": admin_reply, "by": answered_by, "id": inquiry_id},
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
    datasource: str,
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
            "ds": datasource,
        }
        sql = text("""
            INSERT INTO mem_inquiries(
                namespace, prefixes, question, auth_email,
                run_id, research_enabled, research_summary, source_ids,
                status, datasource, created_at, updated_at
            )
            VALUES (
                :ns, :pfx, :q, :mail,
                :run_id, :re, :rs, :src,
                :st, :ds, NOW(), NOW()
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
            "ds": datasource,
        }
        sql2 = text("""
            INSERT INTO mem_inquiries(
                namespace, prefixes, question, auth_email,
                status, datasource, created_at, updated_at
            )
            VALUES (
                :ns, :pfx_txt, :q, :mail,
                :st, :ds, NOW(), NOW()
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
