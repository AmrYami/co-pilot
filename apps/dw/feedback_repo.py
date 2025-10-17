"""Persistence helpers for DW feedback records."""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, Optional

from sqlalchemy import text

from apps.dw.memory_db import ensure_feedback_schema, get_mem_engine

logger = logging.getLogger("dw")

UPSERT_SQL = """
  INSERT INTO dw_feedback(
    inquiry_id, auth_email, rating, comment,
    intent_json, resolved_sql, binds_json, status,
    created_at, updated_at, hints_json
  ) VALUES(
    :inquiry_id, :auth_email, :rating, :comment,
    :intent_json::jsonb, :resolved_sql, :binds_json::jsonb, :status,
    NOW(), NOW(), :hints_json::jsonb
  )
  ON CONFLICT (inquiry_id) DO UPDATE
    SET rating=EXCLUDED.rating,
        comment=EXCLUDED.comment,
        intent_json=EXCLUDED.intent_json,
        resolved_sql=EXCLUDED.resolved_sql,
        binds_json=EXCLUDED.binds_json,
        status=EXCLUDED.status,
        hints_json=EXCLUDED.hints_json,
        auth_email=COALESCE(EXCLUDED.auth_email, dw_feedback.auth_email),
        updated_at=NOW()
  RETURNING id
"""

UPSERT_SQLITE = """
  INSERT INTO dw_feedback(
    inquiry_id, auth_email, rating, comment,
    intent_json, resolved_sql, binds_json, status,
    created_at, updated_at, hints_json
  ) VALUES(
    :inquiry_id, :auth_email, :rating, :comment,
    :intent_json, :resolved_sql, :binds_json, :status,
    CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, :hints_json
  )
  ON CONFLICT(inquiry_id) DO UPDATE SET
    auth_email=COALESCE(:auth_email, dw_feedback.auth_email),
    rating=:rating,
    comment=:comment,
    intent_json=:intent_json,
    resolved_sql=:resolved_sql,
    binds_json=:binds_json,
    status=:status,
    hints_json=:hints_json,
    updated_at=CURRENT_TIMESTAMP
  RETURNING id
"""


def _coerce_payload(
    *,
    inquiry_id: int,
    auth_email: Optional[str],
    rating: Optional[int],
    comment: Optional[str],
    intent: Optional[Dict[str, Any]],
    resolved_sql: Optional[str],
    binds: Optional[Dict[str, Any]],
    hints: Optional[Dict[str, Any]] = None,
    status: str = "pending",
) -> Dict[str, Any]:
    return {
        "inquiry_id": int(inquiry_id or 0),
        "auth_email": (auth_email or "").strip() or None,
        "rating": int(rating or 0),
        "comment": (comment or "").strip() or None,
        "intent_json": json.dumps(intent or {}, default=str, ensure_ascii=False),
        "resolved_sql": resolved_sql or None,
        "binds_json": json.dumps(binds or {}, default=str, ensure_ascii=False),
        "hints_json": json.dumps(hints or {}, default=str, ensure_ascii=False),
        "status": status or "pending",
    }


def _resolve_auth_email(inquiry_id: int, auth_email: Optional[str]) -> Optional[str]:
    email = (auth_email or "").strip()
    if email:
        return email

    try:
        from apps.dw.store import load_inquiry
    except Exception:  # pragma: no cover - optional dependency
        return None

    try:
        inquiry = load_inquiry(inquiry_id)
    except Exception:  # pragma: no cover - defensive
        return None

    if isinstance(inquiry, dict):
        candidate = (inquiry.get("auth_email") or inquiry.get("AUTH_EMAIL") or "").strip()
        if candidate:
            return candidate
    return None


def upsert_feedback(engine, **kwargs) -> Optional[int]:
    """Execute the canonical UPSERT for ``dw_feedback`` using ``inquiry_id``."""

    logger.info(
        "rate.repo.upsert",
        extra={"inquiry_id": kwargs.get("inquiry_id")},
    )
    ensure_feedback_schema(engine)
    dialect = getattr(engine, "dialect", None)
    name = getattr(dialect, "name", "") if dialect is not None else ""
    sql = UPSERT_SQLITE if name.startswith("sqlite") else UPSERT_SQL
    with engine.begin() as cn:
        row = cn.execute(text(sql), kwargs).fetchone()
    return int(row[0]) if row and row[0] is not None else None


def persist_feedback(
    *,
    inquiry_id: int,
    auth_email: Optional[str],
    rating: int,
    comment: str,
    intent: Optional[Dict[str, Any]],
    resolved_sql: Optional[str],
    binds: Optional[Dict[str, Any]],
    hints: Optional[Dict[str, Any]] = None,
    status: str = "pending",
) -> Optional[int]:
    """Backwards compatible helper that resolves the engine and UPSERTs."""

    engine = get_mem_engine()
    payload = _coerce_payload(
        inquiry_id=inquiry_id,
        auth_email=_resolve_auth_email(inquiry_id, auth_email),
        rating=rating,
        comment=comment,
        intent=intent,
        resolved_sql=resolved_sql,
        binds=binds,
        hints=hints,
        status=status,
    )
    return upsert_feedback(engine, **payload)


__all__ = ["persist_feedback", "upsert_feedback", "UPSERT_SQL"]
