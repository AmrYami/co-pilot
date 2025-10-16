"""Shared database helpers for application modules."""

from __future__ import annotations

from typing import Any

from core.settings import Settings
from core.sql_exec import get_mem_engine as _core_get_mem_engine


def get_mem_engine(settings: Any | None = None):
    """Return the shared memory database engine.

    Parameters
    ----------
    settings:
        Optional settings instance used to resolve the memory database URL. When
        omitted, a ``Settings`` accessor scoped to ``dw::common`` is created.
    """

    if settings is None:
        settings = Settings(namespace="dw::common")
    return _core_get_mem_engine(settings)


__all__ = ["get_mem_engine"]
