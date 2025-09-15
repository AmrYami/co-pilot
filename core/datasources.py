from __future__ import annotations

import os
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from core.settings import Settings


def _parse_name_from_url(url: str) -> str:
    try:
        p = urlparse(url)
        # last path segment as db name; strip leading "/"
        name = (p.path or "").lstrip("/").split("/")[-1]
        return name or "app"
    except Exception:
        return "app"


class DatasourceRegistry:
    """
    Builds a registry of SQLAlchemy engines from settings.

    Priority:
      1) mem_settings['DB_CONNECTIONS'] (scope='namespace')
         [
           {"name": "frontaccounting_bk", "url": "...", "role": "oltp"},
           {"name": "membership",         "url": "...", "role": "oltp"}
         ]

      2) mem_settings['APP_DB_URL'] (scope='namespace')  -> single engine
         name = mem_settings['DEFAULT_DATASOURCE'] or dbname from URL or "app"

      3) env: FA_DB_URL or APP_DB_URL                   -> single engine
         name = mem_settings['DEFAULT_DATASOURCE'] or dbname from URL or "app"
    """

    def __init__(self, settings: Settings, namespace: str) -> None:
        self.settings = settings
        self.namespace = namespace
        self.engines: Dict[str, Engine] = {}
        self.meta: Dict[str, Dict[str, Any]] = {}  # store role, url, etc.
        self.default_name: Optional[str] = None

        # 1) Try DB_CONNECTIONS (namespace-scoped JSON)
        conns: List[Dict[str, Any]] = []
        try:
            # `get_json` returns Python objects if the stored value is json/jsonb
            conns = self.settings.get_json(
                "DB_CONNECTIONS", scope="namespace", namespace=self.namespace
            ) or []
        except Exception:
            # Keep going; we'll try fallbacks
            pass

        # 2) Fallback to APP_DB_URL (namespace) if none found
        if not conns:
            app_url = self.settings.get(
                "APP_DB_URL", scope="namespace", namespace=self.namespace
            )
            if app_url:
                name = (
                    self.settings.get(
                        "DEFAULT_DATASOURCE", scope="namespace", namespace=self.namespace
                    )
                    or _parse_name_from_url(app_url)
                    or "app"
                )
                conns = [{"name": name, "url": app_url, "role": "oltp"}]

        # 3) Fallback to environment
        if not conns:
            env_url = os.getenv("FA_DB_URL") or os.getenv("APP_DB_URL")
            if env_url:
                # Try to honor DEFAULT_DATASOURCE if present
                name = (
                    self.settings.get(
                        "DEFAULT_DATASOURCE", scope="namespace", namespace=self.namespace
                    )
                    or _parse_name_from_url(env_url)
                    or "app"
                )
                conns = [{"name": name, "url": env_url, "role": "oltp"}]

        # Build engines
        for entry in conns:
            url = entry.get("url") or entry.get("dsn")
            if not url:
                continue
            name = entry.get("name") or _parse_name_from_url(url) or f"ds_{len(self.engines)+1}"
            role = (entry.get("role") or "oltp").lower()
            # Pool settings are conservative and safe for dev
            engine = create_engine(
                url,
                pool_pre_ping=True,
                pool_recycle=1800,
                pool_size=5,
                max_overflow=10,
                future=True,
            )
            self.engines[name] = engine
            self.meta[name] = {"url": url, "role": role}

        # Decide default
        candidate_default = self.settings.get(
            "DEFAULT_DATASOURCE", scope="namespace", namespace=self.namespace
        )
        if candidate_default and candidate_default in self.engines:
            self.default_name = candidate_default
        elif self.engines:
            # First configured engine
            self.default_name = next(iter(self.engines.keys()))

        if self.engines:
            print(f"[datasources] loaded engines: {list(self.engines.keys())}; default={self.default_name}")
        else:
            print("[datasources] no engines created (check DB_CONNECTIONS or APP_DB_URL).")

    def engine(self, name_or_role: Optional[str]) -> Engine:
        """
        - If None -> default engine
        - If exact name -> that engine
        - If role (e.g. 'oltp') -> first matching by role
        """
        if not self.engines:
            raise RuntimeError("No datasource engine found for requested datasource.")

        if name_or_role is None:
            if not self.default_name:
                raise RuntimeError("No default datasource resolved.")
            return self.engines[self.default_name]

        # name direct match
        if name_or_role in self.engines:
            return self.engines[name_or_role]

        # role match
        role = name_or_role.lower()
        for nm, meta in self.meta.items():
            if meta.get("role") == role:
                return self.engines[nm]

        raise RuntimeError(f"No datasource engine found for '{name_or_role}'. Available: {list(self.engines.keys())}")

    def list(self) -> List[Tuple[str, Dict[str, Any]]]:
        return [(nm, self.meta[nm]) for nm in self.engines.keys()]

