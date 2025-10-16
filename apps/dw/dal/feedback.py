"""Persistence helpers for DW feedback records."""

from __future__ import annotations

import json
from typing import Any, Dict, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session


def _coerce_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _coerce_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _as_json(payload: Any) -> str:
    try:
        return json.dumps(payload or {}, ensure_ascii=False)
    except (TypeError, ValueError):
        return "{}"


def _lookup_auth_email(session: Session, inquiry_id: int) -> str:
    row = (
        session.execute(
            text(
                """
                SELECT COALESCE(auth_email, '') AS auth_email
                  FROM mem_inquiries
                 WHERE id = :id
                """
            ),
            {"id": inquiry_id},
        )
        .mappings()
        .first()
    )
    if not row:
        return ""
    auth_email = row.get("auth_email")
    return _coerce_str(auth_email).strip()


def upsert_feedback(
    session: Session,
    inquiry_id: int,
    rating: int | None,
    comment: str | None,
    intent_obj: Optional[Dict[str, Any]],
    resolved_sql: Optional[str],
    binds_obj: Optional[Dict[str, Any]],
    auth_email: Optional[str],
) -> None:
    """Insert or update a ``dw_feedback`` row using ``inquiry_id`` as the key."""

    if inquiry_id is None:
        raise ValueError("inquiry_id is required for feedback upsert")

    email = (_coerce_str(auth_email).strip() if auth_email else "")
    if not email:
        email = _lookup_auth_email(session, int(inquiry_id))

    payload: Dict[str, Any] = {
        "inquiry_id": int(inquiry_id),
        "auth_email": email,
        "rating": _coerce_int(rating),
        "comment": _coerce_str(comment),
        "intent_json": _as_json(intent_obj),
        "resolved_sql": _coerce_str(resolved_sql),
        "binds_json": _as_json(binds_obj),
    }

    session.execute(
        text(
            """
            INSERT INTO dw_feedback (
                inquiry_id, auth_email, rating, comment,
                intent_json, resolved_sql, binds_json,
                status, created_at, updated_at
            ) VALUES (
                :inquiry_id, :auth_email, :rating, :comment,
                CAST(:intent_json AS JSONB), :resolved_sql, CAST(:binds_json AS JSONB),
                'pending', NOW(), NOW()
            )
            ON CONFLICT (inquiry_id) DO UPDATE SET
                auth_email   = EXCLUDED.auth_email,
                rating       = EXCLUDED.rating,
                comment      = EXCLUDED.comment,
                intent_json  = EXCLUDED.intent_json,
                resolved_sql = EXCLUDED.resolved_sql,
                binds_json   = EXCLUDED.binds_json,
                status       = CASE
                                  WHEN dw_feedback.status IN ('approved', 'rejected')
                                    THEN dw_feedback.status
                                  ELSE 'pending'
                               END,
                updated_at   = NOW()
            """
        ),
        payload,
    )


__all__ = ["upsert_feedback"]
