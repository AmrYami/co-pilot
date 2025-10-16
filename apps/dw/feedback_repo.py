"""Persistence helpers for DW feedback records."""

from __future__ import annotations

import json
from typing import Any, Dict, Optional

from sqlalchemy import text

from apps.common.db import get_mem_engine


def persist_feedback(
    *,
    inquiry_id: int,
    auth_email: str,
    rating: int,
    comment: str,
    intent: Optional[Dict[str, Any]] = None,
    resolved_sql: Optional[str] = None,
    binds: Optional[Dict[str, Any]] = None,
    status: str = "pending",
) -> int:
    """Insert or update a ``dw_feedback`` row and return its identifier."""

    intent_json = json.dumps(intent or {}, ensure_ascii=False)
    binds_json = json.dumps(binds or {}, ensure_ascii=False)

    sql = text(
        """
        INSERT INTO dw_feedback (
            inquiry_id, auth_email, rating, comment,
            intent_json, resolved_sql, binds_json,
            status, created_at, updated_at
        ) VALUES (
            :inquiry_id, :auth_email, :rating, :comment,
            CAST(:intent_json AS JSONB), :resolved_sql, CAST(:binds_json AS JSONB),
            :status, NOW(), NOW()
        )
        ON CONFLICT (inquiry_id) DO UPDATE SET
            rating       = EXCLUDED.rating,
            comment      = EXCLUDED.comment,
            intent_json  = EXCLUDED.intent_json,
            resolved_sql = EXCLUDED.resolved_sql,
            binds_json   = EXCLUDED.binds_json,
            status       = EXCLUDED.status,
            updated_at   = NOW()
        RETURNING id
        """
    )

    engine = get_mem_engine()
    with engine.begin() as conn:
        row = conn.execute(
            sql,
            {
                "inquiry_id": inquiry_id,
                "auth_email": auth_email or "",
                "rating": rating,
                "comment": (comment or "").strip(),
                "intent_json": intent_json,
                "resolved_sql": resolved_sql,
                "binds_json": binds_json,
                "status": status,
            },
        ).first()

    if not row:
        raise RuntimeError("dw_feedback upsert did not return an identifier")

    return int(row[0])


__all__ = ["persist_feedback"]
