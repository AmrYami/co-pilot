# core/inquiries.py
"""
Inquiry helpers: create/update/list feedback entries in mem_inquiries.

These helpers are project-agnostic (no FA-specific code).
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
try:  # pragma: no cover - allow lightweight stubs during testing
    from sqlalchemy.engine import Engine, Row
except Exception:  # pragma: no cover - fallback to satisfy type checking in tests
    class Engine:  # type: ignore[override]
        pass

    class Row:  # type: ignore[override]
        pass
from sqlalchemy import text
import json
try:
    # Guard import so code works even if PostgreSQL dialect isn't installed
    from sqlalchemy.dialects.postgresql import JSONB  # noqa: F401
except Exception:  # pragma: no cover - fallback when dialect missing
    JSONB = None


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


def append_admin_note(mem_engine, inquiry_id: int, by: str, text_note: str) -> int:
    """Append an admin note and update reply metadata safely.

    JSON is sent as text and CAST on the server side to avoid driver quirks.
    """

    note_obj = {"by": by, "text": text_note, "ts": datetime.now(timezone.utc).isoformat()}
    note_txt = json.dumps(note_obj, ensure_ascii=False)
    sql = text(
        """
        UPDATE mem_inquiries
           SET admin_notes = COALESCE(admin_notes, '[]'::jsonb)
                              || jsonb_build_array(CAST(:note AS jsonb)),
               admin_reply = :reply,
               answered_by = :by,
               clarification_rounds = COALESCE(clarification_rounds, 0) + 1,
               updated_at = NOW()
         WHERE id = :id
     RETURNING clarification_rounds
    """
    )
    params = {"id": inquiry_id, "note": note_txt, "reply": text_note, "by": by}
    with mem_engine.begin() as c:
        row = c.execute(sql, params).fetchone()
    return int(row[0]) if row else 0


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
    admin_reply: Optional[str] = None,
    answered_by: Optional[str] = None,
    last_sql: Optional[str] = None,
    last_error: Optional[str] = None,
) -> int:
    """Insert a new inquiry row and return its id.

    JSON values are sent as TEXT and cast server-side to jsonb to avoid driver
    casting issues.
    """

    pfx_txt = json.dumps(prefixes or [], ensure_ascii=False)
    src_txt = json.dumps(source_ids or [], ensure_ascii=False)

    sql = text(
        """
        INSERT INTO mem_inquiries(
            namespace, prefixes, question, auth_email,
            run_id, research_enabled, research_summary, source_ids,
            status, datasource, admin_reply, answered_by,
            last_sql, last_error, created_at, updated_at
        )
        VALUES (
            :ns, :pfx, :q, :mail,
            :run_id, :re, :rs, :src,
            :st, :ds, :reply, :by,
            :last_sql, :last_error, NOW(), NOW()
        )
        RETURNING id
    """
    )

    params: Dict[str, Any] = {
        "ns": namespace,
        "pfx": pfx_txt,
        "q": question,
        "mail": auth_email,
        "run_id": run_id,
        "re": bool(research_enabled),
        "rs": research_summary,
        "src": src_txt,
        "st": status,
        "ds": datasource,
        "reply": admin_reply,
        "by": answered_by,
        "last_sql": last_sql,
        "last_error": last_error,
    }

    with mem_engine.begin() as con:
        row = con.execute(sql, params).fetchone()
    return int(row[0]) if row else 0


def record_rating(
    mem_engine: Engine,
    inquiry_id: int,
    rating: int,
    comment: Optional[str] = None,
) -> None:
    """Persist a user rating (1..5) with an optional free-text comment.

    The helper keeps any existing feedback comment unless a new one is supplied
    and refreshes the ``updated_at`` timestamp to simplify audit queries.
    """

    sql = text(
        """
        UPDATE mem_inquiries
           SET rating = :rating,
               feedback_comment = COALESCE(:comment, feedback_comment),
               updated_at = NOW()
         WHERE id = :inquiry_id
        """
    )
    with mem_engine.begin() as con:
        con.execute(
            sql,
            {"rating": rating, "comment": comment, "inquiry_id": inquiry_id},
        )


def set_feedback(
    mem_engine: Engine,
    *,
    inquiry_id: int,
    satisfied: bool,
    rating: Optional[int],
    comment: Optional[str],
) -> None:
    """Stores user feedback (satisfied / rating / comment)."""
    with mem_engine.begin() as con:
        con.execute(
            text(
                """
            UPDATE mem_inquiries
               SET satisfied=:sat, rating=:rate, feedback_comment=:c, updated_at=NOW()
             WHERE id=:id
        """
            ),
            {"id": inquiry_id, "sat": satisfied, "rate": rating, "c": comment},
        )


def insert_alert(
    mem_engine: Engine,
    namespace: str,
    event_type: str,
    payload: Dict[str, Any],
) -> None:
    """Insert a queued alert row for asynchronous notification channels."""

    sql = text(
        """
        INSERT INTO mem_alerts(namespace, event_type, recipient, payload, status, created_at)
        VALUES (:ns, :et, NULL, CAST(:payload AS jsonb), 'queued', NOW())
        """
    )
    with mem_engine.begin() as con:
        con.execute(sql, {"ns": namespace, "et": event_type, "payload": json.dumps(payload)})


def log_run(
    mem_engine: Engine,
    namespace: str,
    question: str,
    sql_text: str,
    status: str,
    context_pack: Optional[Dict[str, Any]] = None,
) -> Optional[int]:
    """Create a lightweight ``mem_runs`` entry and return its identifier.

    The helper attempts to persist a JSON ``context_pack`` whenever the column is
    available.  When older schemas without that column are in use we gracefully
    fall back to a simplified insert so integrations remain backwards
    compatible.
    """

    ctx_json = json.dumps(context_pack or {}, default=str)
    primary = text(
        """
        INSERT INTO mem_runs(namespace, input_query, sql_text, status, context_pack, created_at)
        VALUES (:ns, :q, :sql, :status, CAST(:ctx AS jsonb), NOW())
        RETURNING id
        """
    )
    fallback = text(
        """
        INSERT INTO mem_runs(namespace, input_query, sql_text, status, created_at)
        VALUES (:ns, :q, :sql, :status, NOW())
        RETURNING id
        """
    )

    with mem_engine.begin() as con:
        try:
            row = con.execute(
                primary,
                {"ns": namespace, "q": question, "sql": sql_text, "status": status, "ctx": ctx_json},
            ).fetchone()
        except Exception:
            row = con.execute(
                fallback,
                {"ns": namespace, "q": question, "sql": sql_text, "status": status},
            ).fetchone()
    return int(row[0]) if row else None


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
