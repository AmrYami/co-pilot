"""Shared database helpers for application modules."""

from __future__ import annotations

from typing import Any

from core.settings import Settings
from core.sql_exec import (
    get_app_engine as _core_get_app_engine,
    get_mem_engine as _core_get_mem_engine,
)


def _ensure_settings(settings: Any | None = None, *, namespace: str = "dw::common") -> Settings:
    if settings is not None and isinstance(settings, Settings):
        return settings
    return Settings(namespace=namespace)


def get_app_engine(settings: Any | None = None, *, namespace: str = "dw::common"):
    """Return the operational database engine for the provided namespace."""

    resolved_settings = _ensure_settings(settings, namespace=namespace)
    return _core_get_app_engine(resolved_settings, namespace)


def get_mem_engine(settings: Any | None = None, *, namespace: str = "dw::common"):
    """Return the shared memory database engine.

    Parameters
    ----------
    settings:
        Optional settings instance used to resolve the memory database URL. When
        omitted, a ``Settings`` accessor scoped to ``dw::common`` is created.
    namespace:
        Namespace used when resolving the database URLs.
    """

    resolved_settings = _ensure_settings(settings, namespace=namespace)
    return _core_get_mem_engine(resolved_settings)


__all__ = ["get_app_engine", "get_mem_engine"]
