from __future__ import annotations
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from core.settings import Settings

class MemoryDB:
    """Lightweight helper around an Engine for mem_settings."""
    def __init__(self, settings: Settings) -> None:
        url = settings.get("MEMORY_DB_URL") or "sqlite://"
        self.engine: Engine = create_engine(url, pool_pre_ping=True, pool_recycle=3600)
