from __future__ import annotations

from sqlalchemy import create_engine
from typing import Any


class DatasourceRegistry:
    def __init__(self, settings, namespace: str):
        self.settings = settings
        self.namespace = namespace
        self._engines: dict[str, Any] = {}
        conns = self.settings.get_json("DB_CONNECTIONS", scope="namespace", default=[]) or []
        if not conns:
            app_url = self.settings.get_str("APP_DB_URL", scope="namespace")
            if app_url:
                self._engines["__default__"] = create_engine(app_url, pool_pre_ping=True)
        else:
            for c in conns:
                name = c.get("name") or c.get("role") or "__default__"
                url = c["url"]
                self._engines[name] = create_engine(url, pool_pre_ping=True)

    def engine(self, name: str | None):
        if name and name in self._engines:
            return self._engines[name]
        dflt = self.settings.get_str("DEFAULT_DATASOURCE", scope="namespace")
        if dflt and dflt in self._engines:
            return self._engines[dflt]
        if len(self._engines) == 1:
            return next(iter(self._engines.values()))
        if "__default__" in self._engines:
            return self._engines["__default__"]
        raise RuntimeError("No datasource engine found for requested datasource.")

