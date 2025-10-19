"""Correlation identifier utilities for request-scoped logging."""
from __future__ import annotations

from contextvars import ContextVar
import uuid


_corr_id: ContextVar[str | None] = ContextVar("corr_id", default=None)


def set_corr_id(value: str | None = None) -> str:
    """Set the current correlation identifier.

    If ``value`` is ``None`` a new identifier is generated using ``uuid4``.
    The generated identifiers are prefixed with ``"req:"`` to make them easy
    to spot in aggregated logs.
    """

    cid = value or f"req:{uuid.uuid4()}"
    _corr_id.set(cid)
    return cid


def get_corr_id() -> str | None:
    """Return the active correlation identifier for the current context."""

    return _corr_id.get()
