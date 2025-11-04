# -*- coding: utf-8 -*-
"""DW admin REST endpoints for feedback moderation and rule management."""
from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, List, Optional, Tuple

from flask import Blueprint, abort, jsonify, request
from sqlalchemy import text
from sqlalchemy.engine import Connection

from apps.dw.db import get_memory_session
from apps.dw.learning import save_positive_rule
from apps.dw.learning_store import _canon_signature_from_intent
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


def _get_admin_emails(conn: Connection, namespace: str | None = None) -> set[str]:
    """Load ADMIN_EMAILS from mem_settings.

    Preference order:
    1) namespace-scoped row for the provided namespace
    2) global-scoped row (any namespace)
    3) fallback to env ADMIN_EMAILS_CSV
    """
    ns = (namespace or "dw::common").strip()

    # 1) Namespace-scoped
    row = conn.execute(
        text(
            """
            SELECT value, value_type
              FROM mem_settings
             WHERE key='ADMIN_EMAILS'
               AND namespace = :ns
             ORDER BY
               CASE WHEN scope='namespace' THEN 0 ELSE 1 END,
               updated_at DESC
             LIMIT 1
            """
        ),
        {"ns": ns},
    ).first()
    # 2) Global-scoped (if nothing found)
    if not row:
        row = conn.execute(
            text(
                """
                SELECT value, value_type
                  FROM mem_settings
                 WHERE key='ADMIN_EMAILS' AND scope='global'
                 ORDER BY updated_at DESC
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

    # Prefer namespace from request for resolving ADMIN_EMAILS row
    ns_for_admin = request.args.get("namespace") or request.headers.get("X-Namespace") or "dw::common"
    if conn is None:
        with eng.begin() as tmp_conn:
            admins = _get_admin_emails(tmp_conn, ns_for_admin)
    else:
        admins = _get_admin_emails(conn, ns_for_admin)

    if auth_email.lower() not in admins:
        abort(403, description="Not in ADMIN_EMAILS")
    return auth_email.lower()


def _row_to_dict(row):
    if hasattr(row, "_mapping"):
        return dict(row._mapping)
    return dict(row)


def _normalize_aggregations_list(value: Any) -> List[Dict[str, Any]]:
    if not isinstance(value, list):
        return []
    normalized: List[Dict[str, Any]] = []
    seen: set[Tuple[str, str, bool, str]] = set()
    for item in value:
        if not isinstance(item, dict):
            continue
        func = str(item.get("func") or "").strip().upper()
        if not func:
            continue
        column_raw = item.get("column")
        if column_raw == "*" or str(column_raw or "").strip() == "*":
            column = "*"
        else:
            column = str(column_raw or "").strip().upper()
        distinct = bool(item.get("distinct"))
        alias_raw = item.get("alias")
        alias = str(alias_raw or "").strip().upper() if alias_raw else None
        key = (func, column, distinct, alias or "")
        if key in seen:
            continue
        seen.add(key)
        normalized.append(
            {
                "func": func,
                "column": column if column else "*",
                "distinct": distinct,
                "alias": alias,
            }
        )
    return normalized


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

                aggregations_norm = _normalize_aggregations_list(intent.get("aggregations"))

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
                numeric_entries = intent.get("numeric_filters") or intent.get("numeric") or []
                if numeric_entries:
                    eq_filters_extended = list(eq_filters)
                    for num_entry in numeric_entries:
                        col = ""
                        op = None
                        values: List[Any] = []
                        if isinstance(num_entry, dict):
                            col = str(num_entry.get("col") or "").strip().upper()
                            op_raw = num_entry.get("op")
                            op = str(op_raw).strip().lower() if op_raw is not None else None
                            raw_vals = num_entry.get("values")
                            if raw_vals is None and num_entry.get("value") is not None:
                                raw_vals = [num_entry.get("value")]
                            if isinstance(raw_vals, (list, tuple, set)):
                                values = list(raw_vals)
                            elif raw_vals is not None:
                                values = [raw_vals]
                        elif isinstance(num_entry, (list, tuple)) and len(num_entry) >= 2:
                            col = str(num_entry[0] or "").strip().upper()
                            op = str(num_entry[1] or "").strip().lower()
                            if len(num_entry) >= 3:
                                vals_part = num_entry[2]
                                if isinstance(vals_part, (list, tuple, set)):
                                    values = list(vals_part)
                                elif vals_part is not None:
                                    values = [vals_part]
                        if not col:
                            continue
                        norm_vals = [
                            v.strip().upper() if isinstance(v, str) else v for v in values
                        ]
                        if op and op not in {"eq", "in"}:
                            eq_filters_extended.append(
                                {
                                    "col": col,
                                    "op": op,
                                    "values": norm_vals,
                                    "ci": False,
                                    "trim": False,
                                }
                            )
                        else:
                            eq_filters_extended.append([col, norm_vals])
                    applied_hints["eq_filters"] = eq_filters_extended
                if aggregations_norm:
                    applied_hints["aggregations"] = aggregations_norm

                has_rule = any(
                    (
                        applied_hints.get("group_by"),
                        applied_hints.get("gross") is not None,
                        applied_hints.get("fts_tokens"),
                        applied_hints.get("eq_filters"),
                        applied_hints.get("aggregations"),
                        applied_hints.get("sort_by"),
                        applied_hints.get("sort_desc") is not None,
                    )
                )

                if question and has_rule:
                    engine = get_memory_engine()
                    # Build signature artifacts from intent when available
                    try:
                        if intent:
                            intent_for_sig = dict(intent)
                            eq_filters_for_sig: List[Any] = []
                            for entry in intent.get("eq_filters") or []:
                                if isinstance(entry, (list, tuple)) and len(entry) == 2:
                                    eq_filters_for_sig.append(
                                        [
                                            str(entry[0]).upper(),
                                            list(entry[1]) if isinstance(entry[1], (list, tuple, set)) else [entry[1]],
                                        ]
                                    )
                                elif isinstance(entry, dict):
                                    col = str(entry.get("col") or entry.get("field") or "").upper()
                                    vals = entry.get("val") if entry.get("val") is not None else entry.get("value")
                                    if vals is None:
                                        vals_list: list[Any] = []
                                    elif isinstance(vals, (list, tuple, set)):
                                        vals_list = list(vals)
                                    else:
                                        vals_list = [vals]
                                    if col:
                                        eq_filters_for_sig.append([col, vals_list])

                            numeric_entries = intent.get("numeric_filters") or intent.get("numeric") or []
                            for num_entry in numeric_entries:
                                col = ""
                                op = None
                                values: List[Any] = []
                                if isinstance(num_entry, dict):
                                    col = str(num_entry.get("col") or "").strip().upper()
                                    op_raw = num_entry.get("op")
                                    op = str(op_raw).strip().lower() if op_raw is not None else None
                                    raw_vals = num_entry.get("values")
                                    if raw_vals is None and num_entry.get("value") is not None:
                                        raw_vals = [num_entry.get("value")]
                                    if isinstance(raw_vals, (list, tuple, set)):
                                        values = list(raw_vals)
                                    elif raw_vals is not None:
                                        values = [raw_vals]
                                elif isinstance(num_entry, (list, tuple)) and len(num_entry) >= 2:
                                    col = str(num_entry[0] or "").strip().upper()
                                    op = str(num_entry[1] or "").strip().lower()
                                    if len(num_entry) >= 3:
                                        vals_part = num_entry[2]
                                        if isinstance(vals_part, (list, tuple, set)):
                                            values = list(vals_part)
                                        elif vals_part is not None:
                                            values = [vals_part]
                                if not col:
                                    continue
                                norm_vals = [
                                    v.strip().upper() if isinstance(v, str) else v for v in values
                                ]
                                if op and op not in {"eq", "in"}:
                                    eq_filters_for_sig.append(
                                        {
                                            "col": col,
                                            "op": op,
                                            "values": norm_vals,
                                            "ci": False,
                                            "trim": False,
                                        }
                                    )
                                else:
                                    eq_filters_for_sig.append([col, norm_vals])

                            if eq_filters_for_sig:
                                def _entry_col(entry: Any) -> str:
                                    if isinstance(entry, (list, tuple)) and entry:
                                        return str(entry[0] or "").upper()
                                    if isinstance(entry, dict):
                                        return str(entry.get("col") or entry.get("field") or "").upper()
                                    return ""

                                def _entry_vals(entry: Any) -> List[Any]:
                                    if isinstance(entry, (list, tuple)) and len(entry) >= 2:
                                        vals = entry[1]
                                        return list(vals) if isinstance(vals, (list, tuple, set)) else [vals]
                                    if isinstance(entry, dict):
                                        raw = entry.get("values")
                                        if raw is None and entry.get("val") is not None:
                                            raw = [entry.get("val")]
                                        if isinstance(raw, (list, tuple, set)):
                                            return list(raw)
                                    return []

                                has_departments = any(_entry_col(e) == "DEPARTMENTS" for e in eq_filters_for_sig)
                                has_department = any(_entry_col(e) == "DEPARTMENT" for e in eq_filters_for_sig)
                                if has_departments and not has_department:
                                    dep_vals = []
                                    for e in eq_filters_for_sig:
                                        if _entry_col(e) == "DEPARTMENTS":
                                            dep_vals = _entry_vals(e)
                                            break
                                    eq_filters_for_sig.append(["DEPARTMENT", list(dep_vals)])
                                intent_for_sig["eq_filters"] = eq_filters_for_sig
                            if intent_for_sig.get("sort_by") and not intent_for_sig.get("order"):
                                intent_for_sig["order"] = {
                                    "col": intent_for_sig.get("sort_by"),
                                    "desc": bool(intent_for_sig.get("sort_desc", True)),
                                }
                            if aggregations_norm:
                                intent_for_sig["aggregations"] = aggregations_norm
                            sha256, _sha1, sig_txt = _canon_signature_from_intent(intent_for_sig)
                            sig_obj = json.loads(sig_txt)
                            sig_sha = sha256
                        else:
                            sig_obj, sig_txt, sig_sha = None, None, None
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
