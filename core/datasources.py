from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional, List
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


@dataclass
class DSConfig:
    name: str
    url: str
    role: str = "oltp"


class DatasourceRegistry:
    def __init__(self, settings, namespace: str):
        self.settings = settings
        self.namespace = namespace
        self._engines: Dict[str, Engine] = {}
        self._default_name: Optional[str] = None

        # 1) Primary: DB_CONNECTIONS (namespace scope)
        conns: List[dict] = self.settings.get_json("DB_CONNECTIONS", scope="namespace") or []

        # 2) Fallback: APP_DB_URL + DEFAULT_DATASOURCE (namespace scope)
        if not conns:
            app_url = self.settings.get("APP_DB_URL", scope="namespace")
            default_name = self.settings.get("DEFAULT_DATASOURCE", scope="namespace") or "default"
            if app_url:
                conns = [{"name": default_name, "url": app_url, "role": "oltp"}]

        # 3) Build engines if we have any connections
        for entry in conns:
            try:
                name = entry.get("name") or "default"
                url = entry.get("url")
                role = entry.get("role") or "oltp"
                if not url:
                    continue
                eng = create_engine(url, pool_pre_ping=True, pool_recycle=3600)
                self._engines[name] = eng
            except Exception as e:
                print(f"[datasources] failed to create engine for {entry}: {e}")

        # Decide default:
        explicit_default = self.settings.get("DEFAULT_DATASOURCE", scope="namespace")
        if explicit_default and explicit_default in self._engines:
            self._default_name = explicit_default
        elif self._engines:
            # First item
            self._default_name = next(iter(self._engines.keys()))
        else:
            self._default_name = None

        if self._engines:
            names = ", ".join(f"{n}" for n in self._engines.keys())
            print(f"[datasources] engines created: {names} (default={self._default_name})")
        else:
            print("[datasources] no engines created (check DB_CONNECTIONS or APP_DB_URL).")

    def engine(self, name: Optional[str]) -> Engine:
        if name:
            eng = self._engines.get(name)
            if eng:
                return eng
            raise RuntimeError(f"No datasource engine found for '{name}'. Available: {list(self._engines.keys())}")
        # default
        if self._default_name and self._default_name in self._engines:
            return self._engines[self._default_name]
        raise RuntimeError("No datasource engine found for requested datasource.")

