"""Persistence helpers for DW feedback that ensure commits and detailed logging."""

from __future__ import annotations

import json
from typing import Any, Dict, Optional

from sqlalchemy import text


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
    """Insert or update a row in ``dw_feedback`` with explicit transaction handling."""

    payload = {
        "inquiry_id": inquiry_id,
        "auth_email": (auth_email or ""),
        "rating": int(rating or 0),
        "comment": (comment or ""),
        "intent_json": json.dumps(intent_dict or {}, ensure_ascii=False),
        "resolved_sql": sql_text or "",
        "binds_json": json.dumps(binds_dict or {}, ensure_ascii=False),
        "status": status or "pending",
    }

    if logger:
        logger.info(
            {
                "event": "rate.persist.attempt",
                "table": "dw_feedback",
                "payload_preview": {
                    key: (
                        value
                        if key in ("inquiry_id", "rating", "status")
                        else f"<len={len(str(value))}>"
                    )
                    for key, value in payload.items()
                },
            }
        )

    with mem_engine.begin() as cx:
        row = cx.execute(
            text(
                """
                INSERT INTO dw_feedback (
                    inquiry_id,
                    auth_email,
                    rating,
                    comment,
                    intent_json,
                    resolved_sql,
                    binds_json,
                    status
                )
                VALUES (
                    :inquiry_id,
                    :auth_email,
                    :rating,
                    :comment,
                    CAST(:intent_json AS JSONB),
                    :resolved_sql,
                    CAST(:binds_json AS JSONB),
                    :status
                )
                ON CONFLICT (inquiry_id) DO UPDATE SET
                    rating = EXCLUDED.rating,
                    comment = EXCLUDED.comment,
                    intent_json = EXCLUDED.intent_json,
                    resolved_sql = EXCLUDED.resolved_sql,
                    binds_json = EXCLUDED.binds_json,
                    status = EXCLUDED.status,
                    updated_at = now()
                RETURNING id
                """
            ),
            payload,
        ).fetchone()

    feedback_id = int(row[0]) if row and row[0] is not None else None

    if logger:
        log_payload = {"event": "rate.persist.ok", "inquiry_id": inquiry_id}
        if feedback_id is not None:
            log_payload["feedback_id"] = feedback_id
        logger.info(log_payload)

    return feedback_id


__all__ = ["upsert_feedback"]
