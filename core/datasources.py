from __future__ import annotations

import logging
from typing import Dict, Optional

from sqlalchemy import create_engine

from core.settings import Settings

log = logging.getLogger(__name__)


class DatasourceRegistry:
    """Registry that builds engines from settings-provided connection details."""

    def __init__(self, settings: Settings, namespace: Optional[str] = None) -> None:
        self.settings = settings
        self.namespace = namespace or getattr(settings, "namespace", "default")
        self._engines: Dict[str, any] = {}
        self._load()

    def _load(self) -> None:
        conns = (
            self.settings.get_json(
                "DB_CONNECTIONS", scope="namespace", namespace=self.namespace
            )
            or []
        )
        for conn in conns:
            name = conn.get("name")
            url = conn.get("url")
            if name and url:
                self._engines[name] = create_engine(
                    url, pool_pre_ping=True, future=True
                )

        if not self._engines:
            fallback_url = self.settings.get(
                "APP_DB_URL", scope="namespace", namespace=self.namespace
            )
            if not fallback_url:
                fallback_url = self.settings.get_string("APP_DB_URL", scope="global")
            if fallback_url:
                self._engines["default"] = create_engine(
                    fallback_url, pool_pre_ping=True, future=True
                )

        if not self._engines:
            log.warning(
                "[datasources] no engines created (check DB_CONNECTIONS or APP_DB_URL)."
            )

    # ------------------------------------------------------------------
    def engine(self, name: Optional[str]) -> any:
        if name and name in self._engines:
            return self._engines[name]

        preferred = self.settings.get(
            "DEFAULT_DATASOURCE", scope="namespace", namespace=self.namespace
        )
        if not preferred:
            preferred = self.settings.get_string("DEFAULT_DATASOURCE", scope="global")
        if preferred and preferred in self._engines:
            return self._engines[preferred]

        if "default" in self._engines:
            return self._engines["default"]

        if len(self._engines) == 1:
            return next(iter(self._engines.values()))

        raise RuntimeError("No datasource engine found for requested datasource.")
