from __future__ import annotations

import os
from typing import Dict, Optional

from sqlalchemy import create_engine


class DatasourceRegistry:
    """Registry that builds engines from settings-provided connection details."""

    def __init__(self, settings, namespace: Optional[str] = None) -> None:
        self.settings = settings
        self.namespace = namespace or getattr(settings, "namespace", "default")
        self._engines: Dict[str, any] = {}

        # Primary path: structured DB_CONNECTIONS within the namespace.
        conns = self.settings.get_json("DB_CONNECTIONS", scope="namespace") or []
        for conn in conns:
            name = conn.get("name")
            url = conn.get("url")
            if not name:
                continue
            if not url:
                continue
            self._engines[name] = create_engine(url, pool_pre_ping=True, future=True)

        # Fallback to a simple APP_DB_URL if no named connections exist.
        if not self._engines:
            app_url = self.settings.get_string("APP_DB_URL", scope="namespace")
            if not app_url:
                app_url = os.getenv("APP_DB_URL")
            if app_url:
                self._engines["default"] = create_engine(app_url, pool_pre_ping=True, future=True)

    # ------------------------------------------------------------------
    def engine(self, name: Optional[str]) -> any:
        if name and name in self._engines:
            return self._engines[name]

        preferred = self.settings.get_string("DEFAULT_DATASOURCE", scope="namespace")
        if preferred and preferred in self._engines:
            return self._engines[preferred]

        if "default" in self._engines:
            return self._engines["default"]

        if self._engines:
            return next(iter(self._engines.values()))

        raise RuntimeError("No datasource engine found for requested datasource.")
