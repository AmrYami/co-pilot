"""Feedback persistence helpers backed by the memory Postgres database."""

from __future__ import annotations

import json
from typing import Any, Dict

from apps.dw.feedback_repo import UPSERT_SQL, upsert_feedback as _repo_upsert


class _Result:
    """Lightweight container for metadata returned by ``upsert_feedback``."""

    def __init__(self, *, rowcount: int | None, inserted_id: Any | None) -> None:
        self.rowcount = rowcount
        self.inserted_id = inserted_id


def _coerce_payload(kwargs: Dict[str, Any]) -> Dict[str, Any]:
    payload = dict(kwargs)
    payload["inquiry_id"] = int(payload.get("inquiry_id") or 0)
    payload["auth_email"] = (payload.get("auth_email") or "").strip() or None
    payload["rating"] = int(payload.get("rating") or 0)
    payload["comment"] = (payload.get("comment") or "").strip() or None
    payload["resolved_sql"] = payload.get("resolved_sql") or None
    payload["status"] = payload.get("status") or "pending"
    payload["intent_json"] = json.dumps(payload.get("intent_json") or {}, default=str)
    payload["binds_json"] = json.dumps(payload.get("binds_json") or {}, default=str)
    return payload


def upsert_feedback(engine, **kwargs: Any) -> _Result:
    """Insert or update a feedback row using the provided SQLAlchemy engine."""

    payload = _coerce_payload(kwargs)
    inserted_id = _repo_upsert(engine, **payload)
    return _Result(rowcount=None, inserted_id=inserted_id)


__all__ = ["upsert_feedback", "UPSERT_SQL"]
