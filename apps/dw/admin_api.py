# -*- coding: utf-8 -*-
"""DW admin REST endpoints for feedback moderation and rule management."""
from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, Optional

from flask import Blueprint, abort, jsonify, request
from sqlalchemy import text
from sqlalchemy.engine import Connection

from apps.dw.db import get_memory_session
from apps.dw.learning import save_positive_rule
from apps.dw.rate_parser import build_intent_signature as _build_sig, signature_text as _sig_text, signature_sha as _sig_sha
from apps.dw.memory_db import get_memory_engine
from apps.dw.order_utils import normalize_order_hint

bp = Blueprint("dw_admin", __name__)

eng = get_memory_engine()


def _flatten_fts_groups(intent: dict) -> list[str]:
    groups = intent.get("fts_groups") or []
    tokens: list[str] = []
    for group in groups:
        if isinstance(group, (list, tuple)):
            tokens.extend([token for token in group if token])
        elif isinstance(group, str):
            token = group.strip()
            if token:
                tokens.append(token)
    return tokens


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


logger = logging.getLogger(__name__)


@bp.post("/feedback/<int:fid>/approve")
def approve_feedback(fid: int):
    body = request.get_json(silent=True) or {}
    admin_note = (body.get("admin_note") or "").strip()
    create_rule = bool(body.get("create_rule"))
    apply_patch = bool(body.get("apply_patch"))

    with get_memory_session() as mem_session:
        conn = mem_session.connection()
        approver = _require_admin(conn)
        logger.info(
            "admin.approve.attempt",
            extra={"feedback_id": fid, "approver": approver},
        )
        rule_created = False
        try:
            row = (
                mem_session.execute(
                    text(
                        """
                        SELECT id, inquiry_id, intent_json, resolved_sql, binds_json
                          FROM dw_feedback
                         WHERE id = :id
                         LIMIT 1
                        """
                    ),
                    {"id": fid},
                )
                .mappings()
                .first()
            )
            if not row:
                mem_session.rollback()
                abort(404, description="feedback not found")

            intent_payload = row.get("intent_json")
            raw_intent: Dict[str, Any]
            if isinstance(intent_payload, dict):
                raw_intent = dict(intent_payload)
            elif isinstance(intent_payload, str) and intent_payload.strip():
                try:
                    parsed = json.loads(intent_payload)
                except json.JSONDecodeError:
                    parsed = {}
                raw_intent = dict(parsed) if isinstance(parsed, dict) else {}
            else:
                raw_intent = {}

            sort_by, sort_desc = normalize_order_hint(
                raw_intent.get("sort_by"), raw_intent.get("sort_desc")
            )
            normalized_intent = dict(raw_intent)
            if sort_by:
                normalized_intent["sort_by"] = sort_by
            else:
                normalized_intent.pop("sort_by", None)
            if sort_desc is None:
                normalized_intent.pop("sort_desc", None)
            else:
                normalized_intent["sort_desc"] = sort_desc

            inquiry_id = row.get("inquiry_id")
            question = ""
            if inquiry_id:
                question_row = (
                    mem_session.execute(
                        text(
                            """
                            SELECT COALESCE(NULLIF(q_norm,''), LOWER(TRIM(question))) AS q
                              FROM mem_inquiries
                             WHERE id = :id
                             LIMIT 1
                            """
                        ),
                        {"id": inquiry_id},
                    )
                    .mappings()
                    .first()
                )
                if question_row:
                    question = (question_row.get("q") or "").strip()
            result = mem_session.execute(
                text(
                    """
                    UPDATE dw_feedback
                       SET status='approved',
                           approver_email=:who,
                           admin_note=:note,
                           updated_at=NOW()
                     WHERE id=:id
                    """
                ),
                {"id": fid, "who": approver, "note": admin_note},
            )
            if result.rowcount is None or result.rowcount == 0:
                mem_session.rollback()
                abort(404, description="feedback not found")
            logger.info(
                "admin.approve.update.ok",
                extra={"feedback_id": fid, "approver": approver},
            )

            make_rule = create_rule or apply_patch
            if make_rule:
                intent = normalized_intent if isinstance(normalized_intent, dict) else {}
                eq_filters_val = intent.get("eq_filters") or []
                if isinstance(eq_filters_val, list):
                    eq_filters = [item for item in eq_filters_val if item is not None]
                else:
                    eq_filters = []

                applied_hints = {
                    "group_by": intent.get("group_by"),
                    "gross": intent.get("gross"),
                    "fts_tokens": _flatten_fts_groups(intent),
                    "eq_filters": eq_filters,
                    "sort_by": intent.get("sort_by"),
                    "sort_desc": bool(intent.get("sort_desc"))
                    if intent.get("sort_desc") is not None
                    else None,
                }

                has_rule = any(
                    (
                        applied_hints.get("group_by"),
                        applied_hints.get("gross") is not None,
                        applied_hints.get("fts_tokens"),
                        applied_hints.get("eq_filters"),
                        applied_hints.get("sort_by"),
                        applied_hints.get("sort_desc") is not None,
                    )
                )

                if question and has_rule:
                    engine = get_memory_engine()
                    # Build signature artifacts from intent when available
                    try:
                        sig_obj = _build_sig(intent) if intent else None
                        sig_txt = _sig_text(sig_obj) if sig_obj else None
                        sig_sha = _sig_sha(sig_obj) if sig_obj else None
                    except Exception:
                        sig_obj, sig_txt, sig_sha = None, None, None
                    save_positive_rule(
                        engine,
                        question,
                        applied_hints,
                        rule_signature=sig_txt,
                        intent_sig=sig_obj,
                        intent_sha=sig_sha,
                    )
                    rule_created = True
                    logger.info(
                        "admin.approve.rule.ok",
                        extra={"feedback_id": fid, "approver": approver},
                    )
                else:
                    logger.info(
                        "admin.approve.rule.skip",
                        extra={"feedback_id": fid, "approver": approver},
                    )

            mem_session.commit()
        except Exception:
            mem_session.rollback()
            logger.exception(
                "admin.approve.fail",
                extra={"feedback_id": fid, "approver": approver},
            )
            raise

    return (
        jsonify(
            {
                "ok": True,
                "id": fid,
                "status": "approved",
                "rule_created": rule_created,
            }
        ),
        200,
    )


@bp.post("/feedback/<int:fid>/reject")
def reject_feedback(fid: int):
    body = request.get_json(silent=True) or {}
    reason = (body.get("reason") or "").strip()
    with get_memory_session() as mem_session:
        conn = mem_session.connection()
        approver = _require_admin(conn)
        result = mem_session.execute(
            text(
                """
                UPDATE dw_feedback
                   SET status='rejected',
                       rejected_reason=:reason,
                       approver_email=:who,
                       updated_at=NOW()
                 WHERE id=:id
                """
            ),
            {"id": fid, "who": approver, "reason": reason},
        )
        if result.rowcount is None or result.rowcount == 0:
            mem_session.rollback()
            abort(404, description="feedback not found")
        mem_session.commit()
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
