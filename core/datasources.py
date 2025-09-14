from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Optional, Any, List
import json
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

@dataclass
class Datasource:
    name: str
    url: str
    role: str = "oltp"

class DatasourceRegistry:
    """
    Builds SQLAlchemy engines from DB_CONNECTIONS setting (namespace-scoped).
    Falls back to single APP_DB_URL if present.
    """
    def __init__(self, settings, namespace: str):
        self.settings = settings
        self.namespace = namespace
        self._engines: Dict[str, Engine] = {}
        self._default_name: Optional[str] = None
        self._load()

    def _load(self):
        conns = self.settings.get("DB_CONNECTIONS", scope="namespace", namespace=self.namespace)
        if not conns:
            # backward compat: APP_DB_URL as the only datasource
            app_url = self.settings.get("APP_DB_URL", scope="namespace", namespace=self.namespace)
            if app_url:
                ds = Datasource(name="default", url=app_url, role="oltp")
                self._engines[ds.name] = self._mk_engine(ds.url)
                self._default_name = ds.name
            return

        for entry in conns:
            name = entry.get("name")
            url  = entry.get("url")
            role = entry.get("role", "oltp")
            if not name or not url:
                continue
            self._engines[name] = self._mk_engine(url)
        default_name = self.settings.get("DEFAULT_DATASOURCE", scope="namespace", namespace=self.namespace) or None
        if default_name and default_name in self._engines:
            self._default_name = default_name
        elif self._engines and not self._default_name:
            # first as default
            self._default_name = list(self._engines.keys())[0]

    def _mk_engine(self, url: str) -> Engine:
        # conservative defaults; MySQL & Postgres friendly
        return create_engine(
            url,
            pool_pre_ping=True,
            pool_recycle=1800,
            future=True,
        )

    def engine(self, name: Optional[str] = None) -> Engine:
        if name is None:
            if not self._default_name:
                raise RuntimeError("No datasource configured")
            name = self._default_name
        if name not in self._engines:
            raise KeyError(f"Unknown datasource: {name}")
        return self._engines[name]

    def list(self) -> List[str]:
        return list(self._engines.keys())

    def default_name(self) -> Optional[str]:
        return self._default_name
