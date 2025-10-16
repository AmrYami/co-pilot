"""Feedback persistence helpers backed by the memory Postgres database."""

from __future__ import annotations

import json
from typing import Any, Dict

from sqlalchemy import text

UPSERT_SQL = text(
    """
INSERT INTO dw_feedback (
  inquiry_id, auth_email, rating, comment,
  intent_json, resolved_sql, binds_json, status
)
VALUES (
  :inquiry_id, :auth_email, :rating, :comment,
  CAST(:intent_json AS JSONB), :resolved_sql, CAST(:binds_json AS JSONB), :status
)
ON CONFLICT (inquiry_id)
DO UPDATE SET
  rating       = EXCLUDED.rating,
  comment      = EXCLUDED.comment,
  intent_json  = EXCLUDED.intent_json,
  resolved_sql = EXCLUDED.resolved_sql,
  binds_json   = EXCLUDED.binds_json,
  status       = EXCLUDED.status,
  updated_at   = now()
RETURNING id
"""
)


class _Result:
    """Lightweight container for metadata returned by ``upsert_feedback``."""

    def __init__(self, *, rowcount: int | None, inserted_id: Any | None) -> None:
        self.rowcount = rowcount
        self.inserted_id = inserted_id


def _coerce_payload(kwargs: Dict[str, Any]) -> Dict[str, Any]:
    payload = dict(kwargs)
    payload["inquiry_id"] = int(payload.get("inquiry_id") or 0)
    payload["auth_email"] = (payload.get("auth_email") or "").strip()
    payload["rating"] = int(payload.get("rating") or 0)
    payload["comment"] = payload.get("comment") or ""
    payload["resolved_sql"] = payload.get("resolved_sql") or ""
    payload["status"] = payload.get("status") or "pending"
    payload["intent_json"] = json.dumps(payload.get("intent_json") or {})
    payload["binds_json"] = json.dumps(payload.get("binds_json") or {})
    return payload


def upsert_feedback(engine, **kwargs: Any) -> _Result:
    """Insert or update a feedback row using the provided SQLAlchemy engine."""

    payload = _coerce_payload(kwargs)
    with engine.begin() as conn:
        result = conn.execute(UPSERT_SQL, payload)
        row = result.mappings().first()
    inserted_id = (row or {}).get("id") if row is not None else None
    return _Result(rowcount=getattr(result, "rowcount", None), inserted_id=inserted_id)


__all__ = ["upsert_feedback", "UPSERT_SQL"]
