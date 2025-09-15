from __future__ import annotations

import os
from typing import Dict, Optional
from sqlalchemy import create_engine

from .settings import Settings


class DatasourceRegistry:
    """Build and cache SQLAlchemy engines from settings."""

    def __init__(self, settings: Settings, namespace: str):
        self.settings = settings
        self.namespace = namespace
        self.engines: Dict[str, any] = {}
        self.default: Optional[str] = self.settings.get_str(
            "DEFAULT_DATASOURCE", namespace=self.namespace, scope="namespace"
        )

        # Preferred: DB_CONNECTIONS [{name, role, url}]
        conns = self.settings.get_json(
            "DB_CONNECTIONS", namespace=self.namespace, scope="namespace"
        ) or []
        for c in conns or []:
            name = c.get("name")
            url = c.get("url")
            if name and url:
                self.engines[name] = create_engine(
                    url, pool_pre_ping=True, pool_recycle=1800
                )
                if not self.default:
                    self.default = name

        # Fallback: APP_DB_URL or env FA_DB_URL
        if not self.engines:
            app_url = self.settings.get_str(
                "APP_DB_URL", namespace=self.namespace, scope="namespace"
            ) or os.environ.get("FA_DB_URL")
            if app_url:
                name = self.default or "default"
                self.engines[name] = create_engine(
                    app_url, pool_pre_ping=True, pool_recycle=1800
                )
                if not self.default:
                    self.default = name

        if not self.engines:
            print("[datasources] no engines created (check DB_CONNECTIONS or APP_DB_URL).")
            raise RuntimeError("No datasource engine found for requested datasource.")

    def engine(self, name: Optional[str]):
        if name and name in self.engines:
            return self.engines[name]
        if self.default and self.default in self.engines:
            return self.engines[self.default]
        if len(self.engines) == 1:
            return next(iter(self.engines.values()))
        raise RuntimeError("No datasource engine found for requested datasource.")

