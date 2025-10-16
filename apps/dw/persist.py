"""Persistence helpers for DW feedback that ensure commits and detailed logging."""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, Optional

from sqlalchemy import text

log = logging.getLogger("dw")

UPSERT_SQL_PG = text(
    """
    INSERT INTO dw_feedback (
      inquiry_id, auth_email, rating, comment,
      intent_json, resolved_sql, binds_json, status, created_at, updated_at
    ) VALUES (
      :inquiry_id, :auth_email, :rating, :comment,
      :intent_json::jsonb, :resolved_sql, :binds_json::jsonb, 'pending', now(), now()
    )
    ON CONFLICT (inquiry_id) DO UPDATE
    SET rating       = EXCLUDED.rating,
        comment      = EXCLUDED.comment,
        intent_json  = EXCLUDED.intent_json,
        resolved_sql = EXCLUDED.resolved_sql,
        binds_json   = EXCLUDED.binds_json,
        status       = EXCLUDED.status,
        updated_at   = now()
    RETURNING id
    """
)


def _coerce_payload(
    *,
    inquiry_id: int,
    auth_email: Optional[str],
    rating: Optional[int],
    comment: Optional[str],
    intent: Optional[Dict[str, Any]],
    final_sql: Optional[str],
    binds: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    return {
        "inquiry_id": int(inquiry_id or 0),
        "auth_email": (auth_email or "").strip(),
        "rating": int(rating or 0),
        "comment": (comment or ""),
        "intent_json": json.dumps(intent or {}, default=str, ensure_ascii=False),
        "resolved_sql": final_sql or "",
        "binds_json": json.dumps(binds or {}, default=str, ensure_ascii=False),
    }


def persist_feedback_to_mem(
    mem_engine,
    *,
    inquiry_id: int,
    auth_email: str,
    rating: int,
    comment: str,
    intent: Dict[str, Any] | None,
    final_sql: str | None,
    binds: Dict[str, Any] | None,
) -> Optional[int]:
    """Persist a feedback payload to the Postgres memory database."""

    payload = _coerce_payload(
        inquiry_id=inquiry_id,
        auth_email=auth_email,
        rating=rating,
        comment=comment,
        intent=intent,
        final_sql=final_sql,
        binds=binds,
    )

    log.info(
        {
            "event": "rate.persist.sql",
            "target": "mem",
            "inquiry_id": inquiry_id,
            "payload_keys": list(payload.keys()),
        }
    )

    with mem_engine.begin() as conn:
        row = conn.execute(UPSERT_SQL_PG, payload).fetchone()

    log.info({"event": "rate.persist.done", "inquiry_id": inquiry_id})

    if row and row[0] is not None:
        try:
            return int(row[0])
        except (TypeError, ValueError):
            return None
    return None


def upsert_feedback(
    mem_engine,
    *,
    inquiry_id: int,
    auth_email: Optional[str],
    rating: Optional[int],
    comment: Optional[str],
    intent_dict: Optional[Dict[str, Any]],
    sql_text: Optional[str],
    binds_dict: Optional[Dict[str, Any]],
    status: str = "pending",
    logger=None,
):
    """Backwards compatible wrapper calling :func:`persist_feedback_to_mem`."""

    if status and status.lower() != "pending":
        log.warning(
            {
                "event": "rate.persist.status_override",
                "status": status,
                "inquiry_id": inquiry_id,
            }
        )

    if logger and logger is not log:
        logger.warning(
            {
                "event": "rate.persist.deprecated_logger",
                "inquiry_id": inquiry_id,
            }
        )

    return persist_feedback_to_mem(
        mem_engine,
        inquiry_id=inquiry_id,
        auth_email=auth_email or "",
        rating=int(rating or 0),
        comment=comment or "",
        intent=intent_dict,
        final_sql=sql_text,
        binds=binds_dict,
    )


__all__ = ["persist_feedback_to_mem", "upsert_feedback", "UPSERT_SQL_PG"]
