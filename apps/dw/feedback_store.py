"""Backwards compatible wrappers around :mod:`apps.dw.feedback_repo`."""

from __future__ import annotations

from typing import Dict, Optional

from apps.dw.feedback_repo import UPSERT_SQL, persist_feedback as _persist_feedback


def persist_feedback(
    *,
    inquiry_id: int,
    auth_email: str,
    rating: int,
    comment: str,
    intent: Dict[str, Any] | None,
    resolved_sql: str | None,
    binds: Dict[str, Any] | None,
    status: str = "pending",
) -> Optional[int]:
    """Delegate to the unified feedback repository implementation."""

    return _persist_feedback(
        inquiry_id=inquiry_id,
        auth_email=auth_email,
        rating=rating,
        comment=comment,
        intent=intent,
        final_sql=resolved_sql,
        binds=binds,
        status=status,
    )


__all__ = ["persist_feedback", "UPSERT_SQL"]
