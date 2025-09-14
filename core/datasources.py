from __future__ import annotations
from typing import Dict, Optional, Any, List
from sqlalchemy import create_engine
from .settings import Settings


class DatasourceRegistry:
    """
    Builds a map {name: sqlalchemy.Engine} from mem_settings.DB_CONNECTIONS.
    Falls back to APP_DB_URL as 'default' when DB_CONNECTIONS is absent.
    """

    def __init__(self, settings: Settings):
        self.settings = settings
        self._engines: Dict[str, Any] = {}
        self._load()

    def _load(self) -> None:
        conns = self.settings.get("DB_CONNECTIONS", scope="namespace") or []
        # Support old shape {name:url}
        if isinstance(conns, dict):
            conns = [{"name": k, "url": v, "role": "oltp"} for k, v in conns.items()]

        for d in conns:
            name = d.get("name")
            url = d.get("url")
            if name and url and name not in self._engines:
                self._engines[name] = create_engine(url, pool_pre_ping=True, future=True)

        # Fallback single-app URL
        app_url = self.settings.get("APP_DB_URL", scope="namespace")
        if app_url and "default" not in self._engines:
            self._engines["default"] = create_engine(app_url, pool_pre_ping=True, future=True)

    def engine(self, name: Optional[str] = None):
        if not name:
            name = self.settings.get("DEFAULT_DATASOURCE", scope="namespace") or "default"
        eng = self._engines.get(name) or self._engines.get("default")
        if not eng:
            raise RuntimeError("No datasource engine found for requested datasource.")
        return eng

    def list(self) -> List[str]:
        return sorted(self._engines.keys())

