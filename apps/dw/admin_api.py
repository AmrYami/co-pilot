# -*- coding: utf-8 -*-
"""DW admin REST endpoints for feedback moderation and rule management."""
from __future__ import annotations

import json
import os
from typing import Optional

from flask import Blueprint, abort, jsonify, request
from sqlalchemy import text
from sqlalchemy.engine import Connection

from apps.dw.db import get_memory_engine, get_memory_session

bp = Blueprint("dw_admin", __name__)

eng = get_memory_engine()


APPROVE_FEEDBACK_SQL = text(
    """
    UPDATE dw_feedback
       SET status='approved',
           approver_email=:admin_email,
           admin_note=:note,
           updated_at=NOW()
     WHERE id = ANY(:ids)
    RETURNING id
    """
)


REJECT_FEEDBACK_SQL = text(
    """
    UPDATE dw_feedback
       SET status='rejected',
           approver_email=:admin_email,
           rejected_reason=:reason,
           updated_at=NOW()
     WHERE id = ANY(:ids)
    RETURNING id
    """
)


def _get_admin_emails(conn: Connection) -> set[str]:
    """Load ADMIN_EMAILS from mem_settings."""
    row = conn.execute(
        text(
            """
            SELECT value, value_type
              FROM mem_settings
             WHERE key='ADMIN_EMAILS'
             ORDER BY
               CASE WHEN scope='namespace' THEN 0 ELSE 1 END,
               key
             LIMIT 1
            """
        )
    ).first()
    if not row:
        fallback = os.getenv("ADMIN_EMAILS_CSV", "")
        if fallback:
            return {part.strip().lower() for part in fallback.split(",") if part.strip()}
        return set()

    mapping = row._mapping if hasattr(row, "_mapping") else row
    val = mapping.get("value") if isinstance(mapping, dict) else row[0]
    try:
        emails = json.loads(val) if isinstance(val, str) else val
    except Exception:
        emails = []
    return {str(e).strip().lower() for e in emails if isinstance(e, str)}


def _require_admin(conn: Optional[Connection] = None) -> str:
    """Ensure the requester is an admin and return the normalized email."""
    body = request.get_json(silent=True) if request.is_json else None
    auth_email = (
        request.headers.get("X-Admin-Email")
        or request.headers.get("X-Auth-Email")
        or request.args.get("admin_email")
        or request.args.get("auth_email")
        or (body.get("auth_email") if isinstance(body, dict) else None)
        or (body.get("admin_email") if isinstance(body, dict) else None)
        or ""
    ).strip()
    if not auth_email:
        abort(401, description="Missing X-Auth-Email")

    if conn is None:
        with eng.begin() as tmp_conn:
            admins = _get_admin_emails(tmp_conn)
    else:
        admins = _get_admin_emails(conn)

    if auth_email.lower() not in admins:
        abort(403, description="Not in ADMIN_EMAILS")
    return auth_email.lower()


def _row_to_dict(row):
    if hasattr(row, "_mapping"):
        return dict(row._mapping)
    return dict(row)


@bp.get("/feedback")
def admin_list_feedback():
    status = (request.args.get("status") or "").strip().lower()
    if status == "all":
        status = ""
    try:
        limit = int(request.args.get("limit", "50"))
    except (TypeError, ValueError):
        limit = 50
    if limit <= 0:
        limit = 50
    limit = min(limit, 500)

    sql = text(
        """
          SELECT id, inquiry_id, auth_email, rating, comment,
                 intent_json, resolved_sql, binds_json,
                 status, approver_email, admin_note, rejected_reason,
                 created_at, updated_at
            FROM dw_feedback
           WHERE (:status = '' OR LOWER(status) = :status)
           ORDER BY created_at DESC
           LIMIT :limit
        """
    )

    with get_memory_session() as mem_session:
        conn = mem_session.connection()
        _require_admin(conn)
        rows = (
            mem_session.execute(sql, {"status": status, "limit": limit})
            .mappings()
            .all()
        )

    return jsonify([_row_to_dict(r) for r in rows])


@bp.post("/feedback/<int:inquiry_id>/approve")
def admin_approve_feedback(inquiry_id: int):
    note = ((request.get_json(silent=True) or {}).get("admin_note") or "").strip()
    with get_memory_session() as mem_session:
        conn = mem_session.connection()
        admin_email = _require_admin(conn)
        mem_session.execute(
            text(
                """
                  UPDATE dw_feedback
                     SET status='approved',
                         approver_email=:user,
                         admin_note=:note,
                         updated_at=now()
                   WHERE inquiry_id=:inq
                """
            ),
            {"user": admin_email, "note": note, "inq": inquiry_id},
        )
        mem_session.commit()
    return jsonify({"ok": True})


@bp.post("/feedback/<int:inquiry_id>/reject")
def admin_reject_feedback(inquiry_id: int):
    reason = ((request.get_json(silent=True) or {}).get("rejected_reason") or "").strip()
    with get_memory_session() as mem_session:
        conn = mem_session.connection()
        admin_email = _require_admin(conn)
        mem_session.execute(
            text(
                """
                  UPDATE dw_feedback
                     SET status='rejected',
                         approver_email=:user,
                         rejected_reason=:reason,
                         updated_at=now()
                   WHERE inquiry_id=:inq
                """
            ),
            {"user": admin_email, "reason": reason, "inq": inquiry_id},
        )
        mem_session.commit()
    return jsonify({"ok": True})


@bp.get("/feedback/<int:fid>")
def get_feedback(fid: int):
    with eng.begin() as conn:
        _require_admin(conn)
        row = conn.execute(
            text(
                """
                SELECT
                  f.id,
                  f.inquiry_id,
                  COALESCE(f.auth_email, i.auth_email) AS auth_email,
                  f.rating,
                  f.comment,
                  f.intent_json,
                  f.resolved_sql,
                  f.binds_json,
                  COALESCE(NULLIF(f.status,''), 'pending') AS status,
                  f.approver_email,
                  f.admin_note,
                  f.rejected_reason,
                  f.created_at,
                  f.updated_at
                FROM dw_feedback f
                LEFT JOIN mem_inquiries i ON i.id = f.inquiry_id
                WHERE f.id = :id
                """
            ),
            {"id": fid},
        ).first()
        if not row:
            abort(404)
    return jsonify(_row_to_dict(row))


@bp.post("/feedback/<int:fid>/approve")
def approve_feedback(fid: int):
    body = request.get_json(silent=True) or {}
    admin_note = (body.get("admin_note") or "").strip()
    apply_patch = bool(body.get("apply_patch") or False)
    with eng.begin() as conn:
        approver = _require_admin(conn)
        result = conn.execute(
            APPROVE_FEEDBACK_SQL,
            {"ids": [fid], "admin_email": approver, "note": admin_note},
        ).mappings().all()
        if not result:
            abort(404, description="feedback not found")

        if apply_patch:
            row = conn.execute(
                text(
                    """
                    SELECT f.inquiry_id, f.resolved_sql, f.intent_json, i.q_norm, i.question
                      FROM dw_feedback f
                      LEFT JOIN mem_inquiries i ON i.id = f.inquiry_id
                     WHERE f.id=:id
                    """
                ),
                {"id": fid},
            ).first()
            if row and row.resolved_sql:
                mapping = row._mapping if hasattr(row, "_mapping") else row
                q_norm = mapping.get("q_norm") or (mapping.get("question") or "").strip().lower()
                if q_norm:
                    conn.execute(
                        text(
                            """
                            INSERT INTO dw_examples (q_norm, sql, success_count, created_at)
                            VALUES (:q, :s, 1, NOW())
                            ON CONFLICT (q_norm) DO UPDATE
                              SET sql=EXCLUDED.sql,
                                  success_count=dw_examples.success_count + 1
                            """
                        ),
                        {"q": q_norm, "s": mapping.get("resolved_sql")},
                    )

    return jsonify({"ok": True, "id": fid, "status": "approved", "applied": apply_patch})


@bp.post("/feedback/<int:fid>/reject")
def reject_feedback(fid: int):
    body = request.get_json(silent=True) or {}
    reason = (body.get("reason") or "").strip()
    with eng.begin() as conn:
        approver = _require_admin(conn)
        result = conn.execute(
            REJECT_FEEDBACK_SQL,
            {"ids": [fid], "admin_email": approver, "reason": reason},
        ).mappings().all()
        if not result:
            abort(404, description="feedback not found")
    return jsonify({"ok": True, "id": fid, "status": "rejected"})


@bp.post("/feedback/<int:fid>/promote")
def promote_feedback(fid: int):
    """Promote approved feedback into dw_examples manually."""
    with eng.begin() as conn:
        _require_admin(conn)
        row = conn.execute(
            text(
                """
                SELECT f.inquiry_id, f.resolved_sql, i.q_norm, i.question
                  FROM dw_feedback f
                  LEFT JOIN mem_inquiries i ON i.id = f.inquiry_id
                 WHERE f.id=:id
                """
            ),
            {"id": fid},
        ).first()
        if not row or not row.resolved_sql:
            abort(400, description="No SQL to promote")

        mapping = row._mapping if hasattr(row, "_mapping") else row
        q_norm = mapping.get("q_norm") or (mapping.get("question") or "").strip().lower()
        if not q_norm:
            abort(400, description="No question/q_norm to promote")

        conn.execute(
            text(
                """
                INSERT INTO dw_examples (q_norm, sql, success_count, created_at)
                VALUES (:q, :s, 1, NOW())
                ON CONFLICT (q_norm) DO UPDATE
                  SET sql=EXCLUDED.sql,
                      success_count=dw_examples.success_count + 1
                """
            ),
            {"q": q_norm, "s": mapping.get("resolved_sql")},
        )

    return jsonify({"ok": True, "promoted": True, "id": fid})


@bp.get("/rules")
def list_rules():
    ns = request.args.get("namespace", "dw::common")
    with eng.begin() as conn:
        _require_admin(conn)
        rows = conn.execute(
            text(
                """
                SELECT id, namespace, pattern, rule, weight, author_email, approved_by,
                       created_at, updated_at
                  FROM dw_rules
                 WHERE namespace=:ns
                 ORDER BY created_at DESC
                 LIMIT 1000
                """
            ),
            {"ns": ns},
        ).mappings().all()
    return jsonify({"ok": True, "rows": [dict(r) for r in rows]})


@bp.post("/rules")
def create_rule():
    body = request.get_json(force=True)
    ns = body.get("namespace", "dw::common")
    pattern = body.get("pattern")
    rule = body.get("rule")
    weight = body.get("weight", 0.5)
    if not pattern or not rule:
        abort(400, description="pattern and rule are required")

    with eng.begin() as conn:
        admin = _require_admin(conn)
        rid = conn.execute(
            text(
                """
                INSERT INTO dw_rules(namespace, pattern, rule, weight, author_email, approved_by, created_at)
                VALUES(:ns, :pattern, :rule::jsonb, :w, :author, :admin, NOW())
                RETURNING id
                """
            ),
            {
                "ns": ns,
                "pattern": pattern,
                "rule": json.dumps(rule),
                "w": weight,
                "author": admin,
                "admin": admin,
            },
        ).scalar_one()
    return jsonify({"ok": True, "id": rid})


__all__ = ["bp"]
