"""Database connection helpers for DW routes."""

from __future__ import annotations

import os
import threading
from typing import Any, Dict, List, Optional

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from apps.settings import get_setting

_MEMORY_ENGINE: Engine | None = None
_MEMORY_URL: str | None = None
_MEMORY_LOCK = threading.Lock()

_APP_ENGINES: Dict[str, Engine] = {}
_APP_URLS: Dict[str, str] = {}
_APP_LOCK = threading.Lock()


def _coerce_url(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        url = value.strip()
        return url or None
    return str(value)


def _resolve_memory_url() -> str:
    url = _coerce_url(get_setting("MEMORY_DB_URL", scope="global"))
    if url:
        return url
    env_url = os.getenv("MEMORY_DB_URL", "").strip()
    if env_url:
        return env_url
    raise RuntimeError("MEMORY_DB_URL is not configured in mem_settings or environment")


def get_memory_engine() -> Engine:
    """Return a cached SQLAlchemy engine for the Postgres memory database."""

    global _MEMORY_ENGINE, _MEMORY_URL
    url = _resolve_memory_url()
    if _MEMORY_ENGINE is not None and _MEMORY_URL == url:
        return _MEMORY_ENGINE

    with _MEMORY_LOCK:
        if _MEMORY_ENGINE is not None and _MEMORY_URL == url:
            return _MEMORY_ENGINE
        engine = create_engine(url, pool_pre_ping=True, future=True)
        _MEMORY_ENGINE = engine
        _MEMORY_URL = url
        return engine


def _resolve_app_url(namespace: str = "dw::common") -> str:
    scopes = ("namespace", "global")
    for scope in scopes:
        url = _coerce_url(get_setting("APP_DB_URL", scope=scope, namespace=namespace))
        if url:
            return url
    env_url = os.getenv("APP_DB_URL", "").strip()
    if env_url:
        return env_url
    raise RuntimeError("APP_DB_URL is not configured in mem_settings or environment")


def get_app_engine(namespace: str = "dw::common") -> Engine:
    """Return a cached SQLAlchemy engine for the operational Oracle datasource."""

    key = namespace or "dw::common"
    cached = _APP_ENGINES.get(key)
    cached_url = _APP_URLS.get(key)
    url = _resolve_app_url(namespace=key)
    if cached is not None and cached_url == url:
        return cached

    with _APP_LOCK:
        cached = _APP_ENGINES.get(key)
        cached_url = _APP_URLS.get(key)
        if cached is not None and cached_url == url:
            return cached
        engine = create_engine(url, pool_pre_ping=True, future=True)
        _APP_ENGINES[key] = engine
        _APP_URLS[key] = url
        return engine


def get_engine(name: Optional[str] = None, *, namespace: str = "dw::common") -> Engine:
    """Compatibility wrapper returning the main application datasource engine."""

    return get_app_engine(namespace=namespace)


def fetch_rows(sql: Any, binds: Dict[str, Any] | None = None) -> List[Dict[str, Any]]:
    """Execute ``sql`` against the application datasource and return rows.

    The helper intentionally keeps a very small surface area so that unit tests can
    monkeypatch it when a real database is not available.
    """

    try:
        engine = get_app_engine()
    except Exception:
        return []

    statement = sql if hasattr(sql, "compile") else text(str(sql))
    effective_binds = dict(binds or {})

    with engine.connect() as conn:
        result = conn.execute(statement, effective_binds)  # type: ignore[arg-type]
        columns = list(result.keys())
        return [dict(zip(columns, row)) for row in result.fetchall()]


__all__ = ["fetch_rows", "get_app_engine", "get_engine", "get_memory_engine"]
