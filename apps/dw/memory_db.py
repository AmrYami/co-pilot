"""Memory database connection helpers."""

from __future__ import annotations

from sqlalchemy.engine import Engine

from apps.core.memdb import get_memory_engine


def get_mem_engine() -> Engine:
    """Return the shared Postgres-backed memory engine."""

    return get_memory_engine()


__all__ = ["get_mem_engine"]
