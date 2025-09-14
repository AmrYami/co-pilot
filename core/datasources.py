from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional, Any, List
from sqlalchemy import create_engine
from urllib.parse import urlparse
import re


@dataclass
class DSConfig:
    name: str
    url: str
    role: str = "oltp"


def _dbname_from_url(url: str) -> str:
    # mysql+pymysql://user:pass@host/dbname?charset=utf8mb4
    try:
        p = urlparse(url)
        # path like "/dbname"
        db = (p.path or "/").lstrip("/") or "app"
        # strip query decorations if any (usually not in path)
        db = re.split(r"[?#]", db, 1)[0]
        return db
    except Exception:
        return "app"


class DatasourceRegistry:
    """
    Loads datasource engines from settings:
      - Prefer DB_CONNECTIONS (array of {name,url,role})
      - Else fallback to APP_DB_URL as a single engine
      - DEFAULT_DATASOURCE chooses the default; else only engine name
    """

    def __init__(self, settings):
        self.settings = settings
        self.engines: Dict[str, Any] = {}
        self.default_name: Optional[str] = None
        self._build()

    def _build(self) -> None:
        # Try multi-DS first
        conns: List[dict] = self.settings.get("DB_CONNECTIONS") or []

        if not conns:
            # Fallback to single app DS
            app_url: Optional[str] = self.settings.get("APP_DB_URL")
            if app_url:
                default_name = (
                    self.settings.get("DEFAULT_DATASOURCE")
                    or _dbname_from_url(app_url)
                    or "app"
                )
                conns = [{"name": default_name, "url": app_url, "role": "oltp"}]

        # Build engines
        for c in conns:
            name = str(c.get("name") or "").strip()
            url = str(c.get("url") or "").strip()
            if not name or not url:
                continue
            try:
                self.engines[name] = create_engine(url, pool_pre_ping=True)
            except Exception as e:
                print(f"[datasources] failed to create engine for {name}: {e}")

        # Choose default
        self.default_name = self.settings.get("DEFAULT_DATASOURCE")
        if not self.default_name and self.engines:
            # if only one engine, make it default
            if len(self.engines) == 1:
                self.default_name = next(iter(self.engines.keys()))

        # Log what we have
        if not self.engines:
            print("[datasources] no engines created (check DB_CONNECTIONS or APP_DB_URL).")
        else:
            print(f"[datasources] engines: {list(self.engines.keys())}, default={self.default_name}")

    def engine(self, name: Optional[str]) -> Any:
        # Allow None / "" to mean "give me the default"
        if not name:
            name = self.default_name
        if not name and self.engines:
            # Last chance: single engine scenario
            if len(self.engines) == 1:
                return next(iter(self.engines.values()))
        if not name or name not in self.engines:
            raise RuntimeError("No datasource engine found for requested datasource.")
        return self.engines[name]

