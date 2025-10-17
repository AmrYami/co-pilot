"""Helpers for working with DW feedback records."""

from __future__ import annotations

from typing import Any, Dict, Optional

from apps.dw.feedback_store import persist_feedback as _persist_feedback

__all__ = ["STATUS_ALIASES", "normalize_status", "persist_feedback"]


# Canonical statuses accepted by the feedback endpoints.
_CANONICAL_STATUSES = {"pending", "approved", "rejected"}


# Common aliases that map to the canonical statuses. Keep everything in lower
# case so normalization can be a simple dictionary lookup.
STATUS_ALIASES = {
    "": "",
    "pend": "pending",
    "pending": "pending",
    "pending_review": "pending",
    "approve": "approved",
    "approved": "approved",
    "ok": "approved",
    "reject": "rejected",
    "rejected": "rejected",
}


def normalize_status(value: Optional[str], *, default: str = "") -> str:
    """Normalize ``value`` to one of the canonical statuses.

    Parameters
    ----------
    value:
        Raw status input from query parameters or JSON payloads.
    default:
        Fallback when ``value`` does not map to a known status. For example the
        list endpoint wants an empty string to mean "all statuses", while the
        rating endpoint defaults to ``pending`` when nothing is supplied.
    """

    key = (value or "").strip().lower()
    if not key:
        return default

    mapped = STATUS_ALIASES.get(key)
    if mapped:
        return mapped

    if key in _CANONICAL_STATUSES:
        return key

    return default


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
    """Persist feedback using the canonical repository implementation."""

    return _persist_feedback(
        inquiry_id=inquiry_id,
        auth_email=auth_email,
        rating=rating,
        comment=comment,
        intent=intent,
        resolved_sql=resolved_sql,
        binds=binds,
        status=status,
    )

