from __future__ import annotations
import threading
from typing import Dict, Optional
from sqlalchemy import create_engine

from .settings import get_db_connections, get_default_datasource


class SqlRouter:
    """Caches SQLAlchemy engines by logical datasource name."""

    def __init__(self, settings):
        self._settings = settings
        self._lock = threading.Lock()
        self._engines: Dict[str, any] = {}
        self._sources: Dict[str, str] = {}
        self._default: Optional[str] = None
        self._load()

    def _load(self):
        self._sources.clear()
        self._engines.clear()
        default = get_default_datasource(self._settings)
        for s in get_db_connections(self._settings):
            name = s["name"]
            self._sources[name] = s["url"]
            if s.get("default") and not default:
                default = name
        self._default = default

    def reload(self):
        with self._lock:
            self._load()

    def list(self) -> dict:
        return {"default": self._default, "sources": list(self._sources.keys())}

    def get_engine(self, name: Optional[str] = None):
        key = name or self._default
        if not key:
            raise RuntimeError("No default datasource configured.")
        with self._lock:
            eng = self._engines.get(key)
            if eng is None:
                url = self._sources.get(key)
                if not url:
                    raise KeyError(f"Datasource not found: {key}")
                eng = create_engine(url, pool_pre_ping=True)
                self._engines[key] = eng
            return eng
