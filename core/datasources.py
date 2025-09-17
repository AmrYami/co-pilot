from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional

from sqlalchemy import create_engine

from core.settings import Settings


@dataclass
class DatasourceRegistry:
    settings: Settings
    namespace: str

    def __post_init__(self) -> None:
        self._engines: Dict[str, any] = {}

        conns = self.settings.get_json("DB_CONNECTIONS", scope="namespace", default=[]) or []
        if not conns:
            single = (
                self.settings.get_str("APP_DB_URL", scope="namespace", default=None)
                or self.settings.get_str("APP_DB_URL", scope="global", default=None)
            )
            if single:
                self._engines["default"] = create_engine(single, pool_pre_ping=True)
        else:
            for c in conns:
                name = c.get("name") or "default"
                url = c.get("url")
                if url:
                    self._engines[name] = create_engine(url, pool_pre_ping=True)

        if not self._engines:
            print("[datasources] no engines created (check DB_CONNECTIONS or APP_DB_URL).")

    def engine(self, which: Optional[str]) -> any:
        if which and which in self._engines:
            return self._engines[which]

        default_name = self.settings.get_str("DEFAULT_DATASOURCE", scope="namespace", default=None)
        if default_name and default_name in self._engines:
            return self._engines[default_name]

        if len(self._engines) == 1:
            return list(self._engines.values())[0]

        raise RuntimeError("No datasource engine found for requested datasource.")
