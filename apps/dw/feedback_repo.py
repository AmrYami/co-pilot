"""Persistence helpers for DW feedback records."""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, Optional

from sqlalchemy import text

from apps.dw.memory_db import get_mem_engine

log = logging.getLogger("dw")

UPSERT_SQL = text(
    """
INSERT INTO dw_feedback (
  inquiry_id, auth_email, rating, comment,
  intent_json, resolved_sql, binds_json, status
)
VALUES (
  :inquiry_id, :auth_email, :rating, :comment,
  CAST(:intent_json AS JSONB), :resolved_sql, CAST(:binds_json AS JSONB), 'pending'
)
ON CONFLICT (inquiry_id) DO UPDATE SET
  auth_email   = EXCLUDED.auth_email,
  rating       = EXCLUDED.rating,
  comment      = EXCLUDED.comment,
  intent_json  = EXCLUDED.intent_json,
  resolved_sql = EXCLUDED.resolved_sql,
  binds_json   = EXCLUDED.binds_json,
  updated_at   = now()
RETURNING id
    """
)


def persist_feedback(
    *,
    inquiry_id: int,
    auth_email: str,
    rating: int,
    comment: str,
    intent: Optional[Dict[str, Any]],
    final_sql: Optional[str],
    binds: Optional[Dict[str, Any]],
) -> Optional[int]:
    """Insert or update a ``dw_feedback`` row and return its identifier."""

    engine = get_mem_engine()
    intent_json = json.dumps(intent or {})
    binds_json = json.dumps(binds or {})

    params = {
        "inquiry_id": inquiry_id,
        "auth_email": auth_email or "",
        "rating": int(rating or 0),
        "comment": comment or "",
        "intent_json": intent_json,
        "resolved_sql": final_sql or None,
        "binds_json": binds_json,
    }

    log.info(
        {
            "event": "rate.persist.attempt",
            "inquiry_id": inquiry_id,
            "auth_email": auth_email,
            "rating": rating,
        }
    )

    with engine.begin() as conn:
        row = conn.execute(UPSERT_SQL, params).fetchone()
        feedback_id = int(row[0]) if row and row[0] is not None else None
        log.info(
            {
                "event": "rate.persist.ok",
                "inquiry_id": inquiry_id,
                "feedback_id": feedback_id,
            }
        )
        return feedback_id


__all__ = ["persist_feedback"]
