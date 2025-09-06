from __future__ import annotations
import threading
from typing import Dict, Optional
from sqlalchemy import create_engine

from .settings import get_db_sources


class SqlRouter:
    """Cache SQLAlchemy engines by logical datasource name."""

    def __init__(self, settings) -> None:
        self._settings = settings
        self._lock = threading.Lock()
        self._engines: Dict[str, any] = {}
        self._default_name: Optional[str] = None
        self._load_from_settings()

    def _load_from_settings(self) -> None:
        self._sources: Dict[str, str] = {}
        default_name: Optional[str] = None
        for s in get_db_sources(self._settings):
            name = s.get("name")
            if not name:
                continue
            self._sources[name] = s.get("url")
            if s.get("default") and not default_name:
                default_name = name
        self._default_name = default_name

    def reload(self) -> None:
        with self._lock:
            self._engines.clear()
            self._load_from_settings()

    def get_engine(self, name: Optional[str] = None):
        key = name or self._default_name
        if not key:
            raise RuntimeError("No default datasource configured")
        with self._lock:
            eng = self._engines.get(key)
            if eng is None:
                url = self._sources.get(key)
                if not url:
                    raise KeyError(f"Datasource not found: {key}")
                eng = create_engine(url, pool_pre_ping=True)
                self._engines[key] = eng
            return eng

    def list(self) -> dict:
        return {"default": self._default_name, "sources": list(self._sources.keys())}
