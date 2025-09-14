from __future__ import annotations

from typing import Dict, Optional
from sqlalchemy import create_engine
from .settings import Settings


class DatasourceRegistry:
    """
    Builds SQLAlchemy engines from mem_settings:
      - DB_CONNECTIONS: [{"name":"frontaccounting_bk","url":"...","role":"oltp"}, ...]
      - DEFAULT_DATASOURCE: name to use when none is requested
      - APP_DB_URL: fallback single-URL when DB_CONNECTIONS is not set
      - FA_DB_URL: final env fallback
    """

    def __init__(self, settings: Settings, namespace: str):
        self.settings = settings
        self.namespace = namespace
        self._engines: Dict[str, any] = {}
        self.default_name: Optional[str] = None

        conns = self.settings.get_json("DB_CONNECTIONS", scope="namespace") or []
        default_ds = self.settings.get("DEFAULT_DATASOURCE", scope="namespace")
        app_url = (
            self.settings.get("APP_DB_URL", scope="namespace")
            or self.settings.get("APP_DB_URL")                       # global
            or self.settings.get("FA_DB_URL")                        # env bridge
        )

        if conns:
            for c in conns:
                name = (c.get("name") or "").strip()
                url = (c.get("url") or "").strip()
                if not name or not url:
                    continue
                self._engines[name] = create_engine(url, pool_pre_ping=True)
            # pick default
            if default_ds and default_ds in self._engines:
                self.default_name = default_ds
            elif self._engines:
                self.default_name = next(iter(self._engines.keys()))
        elif app_url:
            # If we only have one URL, expose it under DEFAULT_DATASOURCE if present, else 'app'
            name = default_ds or "app"
            self._engines[name] = create_engine(app_url, pool_pre_ping=True)
            self.default_name = name
        else:
            print("[datasources] no engines created (check DB_CONNECTIONS or APP_DB_URL).")

    def engine(self, name: Optional[str]) -> any:
        key = name or self.default_name
        if key and key in self._engines:
            return self._engines[key]
        raise RuntimeError("No datasource engine found for requested datasource.")

    def engines(self) -> Dict[str, any]:
        return dict(self._engines)

