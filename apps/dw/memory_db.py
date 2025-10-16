"""Memory database connection helpers."""

from __future__ import annotations

import logging
import os
from functools import lru_cache

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

log = logging.getLogger("dw")


def _resolve_mem_url() -> str:
    """Resolve the memory database URL from environment or settings."""

    url = os.getenv("MEMORY_DB_URL") or os.getenv("MEM_DB_URL")
    if not url:
        try:
            from apps.dw.settings import get_setting  # type: ignore

            url = get_setting("MEMORY_DB_URL", default="")
        except Exception:  # pragma: no cover - optional settings backend
            url = ""
    if not url:
        raise RuntimeError("MEMORY_DB_URL is not set. Cannot persist feedback.")
    return url


@lru_cache(maxsize=1)
def get_mem_engine() -> Engine:
    """Return a cached SQLAlchemy engine for the memory database."""

    url = _resolve_mem_url()
    echo = os.getenv("MEM_SQL_ECHO", "false").lower() == "true"
    engine = create_engine(url, pool_pre_ping=True, echo=echo, future=True)
    log.info({"event": "mem.engine.init", "url": url, "echo": echo})
    return engine


__all__ = ["get_mem_engine"]
