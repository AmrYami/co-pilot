"""Compat helpers delegating to :mod:`apps.dw.feedback_repo`."""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from apps.dw.feedback_repo import persist_feedback as _persist_feedback
from apps.dw.memory_db import ensure_feedback_schema, get_mem_engine

log = logging.getLogger("dw")


def ensure_schema() -> None:
    """Ensure the ``dw_feedback`` schema exists in the memory database."""

    ensure_feedback_schema(get_mem_engine())


def save_feedback(
    inquiry_id: int,
    rating: int,
    comment: Optional[str],
    hints_payload: Optional[Dict[str, Any]] = None,
    *,
    auth_email: Optional[str] = None,
    binds_payload: Optional[Dict[str, Any]] = None,
    resolved_sql: Optional[str] = None,
    status: str = "pending",
) -> Optional[int]:
    """Persist feedback rows via the unified repository implementation."""

    ensure_schema()
    log.info({"event": "rate.persist.compat", "inquiry_id": inquiry_id})
    return _persist_feedback(
        inquiry_id=inquiry_id,
        auth_email=auth_email,
        rating=rating,
        comment=comment or "",
        intent=hints_payload or {},
        resolved_sql=resolved_sql,
        binds=binds_payload,
        hints=hints_payload,
        status=status,
    )


__all__ = ["ensure_schema", "save_feedback"]
