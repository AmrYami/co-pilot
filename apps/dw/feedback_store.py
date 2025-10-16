"""Utilities for persisting DocuWare feedback to the memory database."""

from __future__ import annotations

import json
import logging
from typing import Any, Dict

from sqlalchemy import text

from apps.dw.memory_db import get_mem_engine

log = logging.getLogger("dw")

UPSERT_SQL = text(
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
  auth_email    = EXCLUDED.auth_email,
  rating        = EXCLUDED.rating,
  comment       = EXCLUDED.comment,
  intent_json   = EXCLUDED.intent_json,
  resolved_sql  = EXCLUDED.resolved_sql,
  binds_json    = EXCLUDED.binds_json,
  updated_at    = NOW()
RETURNING id
    """
)


def _coerce_payload(
    *,
    inquiry_id: int,
    auth_email: str,
    rating: int,
    comment: str,
    intent: Dict[str, Any] | None,
    resolved_sql: str | None,
    binds: Dict[str, Any] | None,
) -> Dict[str, Any]:
    return {
        "inquiry_id": int(inquiry_id or 0),
        "auth_email": (auth_email or "").strip(),
        "rating": int(rating or 0),
        "comment": (comment or "").strip(),
        "intent_json": json.dumps(intent or {}, ensure_ascii=False),
        "resolved_sql": resolved_sql or "",
        "binds_json": json.dumps(binds or {}, ensure_ascii=False),
    }


def persist_feedback(
    *,
    inquiry_id: int,
    auth_email: str,
    rating: int,
    comment: str,
    intent: Dict[str, Any] | None,
    resolved_sql: str | None,
    binds: Dict[str, Any] | None,
) -> int:
    """Insert or update a ``dw_feedback`` record and return its identifier."""

    if not inquiry_id:
        raise ValueError("inquiry_id is required for feedback persistence")

    engine = get_mem_engine()

    payload = _coerce_payload(
        inquiry_id=inquiry_id,
        auth_email=auth_email,
        rating=rating,
        comment=comment,
        intent=intent,
        resolved_sql=resolved_sql,
        binds=binds,
    )

    log.info(
        {
            "event": "rate.persist.attempt",
            "inquiry_id": inquiry_id,
            "auth_email": payload["auth_email"],
            "rating": payload["rating"],
        }
    )

    with engine.begin() as conn:
        result = conn.execute(UPSERT_SQL, payload)
        feedback_id = int(result.scalar_one())

    log.info(
        {
            "event": "rate.persist.ok",
            "inquiry_id": inquiry_id,
            "feedback_id": feedback_id,
        }
    )

    return feedback_id


__all__ = ["persist_feedback", "UPSERT_SQL"]
