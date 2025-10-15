"""Helpers for working with DW feedback records."""

from __future__ import annotations

from typing import Optional

__all__ = ["STATUS_ALIASES", "normalize_status"]


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

