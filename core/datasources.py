# core/datasources.py
from __future__ import annotations

import json
import os
from typing import Any, Dict, Optional

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


class DatasourceRegistry:
    """
    Builds a pool of SQLAlchemy engines from settings.

    Priority:
      1) DB_CONNECTIONS  (array of {"name","url","role"})
      2) APP_DB_URL      (single URL; name = DEFAULT_DATASOURCE or 'app')
      3) env APP_DB_URL / FA_DB_URL (same fallback as #2)

    Default selection:
      - DEFAULT_DATASOURCE if present
      - else: if exactly one engine exists, use that
    """

    def __init__(self, settings: Any, namespace: Optional[str] = None) -> None:
        self.settings = settings
        # keep the namespace for potential future scoping; Settings typically already resolves scope
        self.namespace = namespace or getattr(settings, "namespace", None)
        self.engines: Dict[str, Engine] = {}
        self.default_name: Optional[str] = None

        self._load_from_settings()

    # --- public API ---------------------------------------------------------

    def engine(self, name: Optional[str]) -> Engine:
        """
        Return an engine by name. If name is None, use the configured default.
        If only one engine exists, that becomes the default implicitly.
        """
        target = name or self.default_name or (next(iter(self.engines)) if len(self.engines) == 1 else None)
        if target and target in self.engines:
            return self.engines[target]
        raise RuntimeError("No datasource engine found for requested datasource.")

    def has_any(self) -> bool:
        return bool(self.engines)

    # --- internals ----------------------------------------------------------

    def _load_from_settings(self) -> None:
        raw_conns = self.settings.get("DB_CONNECTIONS", None)
        default_ds = self.settings.get("DEFAULT_DATASOURCE", None)

        # Parse DB_CONNECTIONS if it arrived as a JSON string
        connections: Optional[list] = None
        if raw_conns:
            if isinstance(raw_conns, str):
                try:
                    connections = json.loads(raw_conns)
                except Exception:
                    print("[datasources] WARNING: DB_CONNECTIONS is a string but not valid JSON; ignoring.")
                    connections = None
            elif isinstance(raw_conns, (list, tuple)):
                connections = list(raw_conns)
            else:
                print("[datasources] WARNING: DB_CONNECTIONS has unexpected type; ignoring.")

        # Fallback to APP_DB_URL (settings) or env
        app_url = self.settings.get("APP_DB_URL", None) or os.getenv("APP_DB_URL") or os.getenv("FA_DB_URL")

        if not connections and app_url:
            # synthesize a single entry
            name = default_ds or "app"
            connections = [{"name": name, "url": app_url, "role": "oltp"}]
            if not default_ds:
                default_ds = name

        if not connections:
            print("[datasources] no engines created (check DB_CONNECTIONS or APP_DB_URL).")
            self.default_name = None
            self.engines = {}
            return

        created = []
        for entry in connections:
            try:
                name = entry.get("name")
                url = entry.get("url")
                if not name or not url:
                    continue
                # reasonable pool defaults; adjust if needed per RDS/MySQL
                engine = create_engine(
                    url,
                    pool_pre_ping=True,
                    pool_recycle=1800,
                    pool_size=5,
                    max_overflow=10,
                )
                self.engines[name] = engine
                created.append(name)
            except Exception as e:
                print(f"[datasources] failed creating engine for {entry!r}: {e}")

        # Decide default
        self.default_name = default_ds or (created[0] if len(created) == 1 else None)

        if not self.engines:
            print("[datasources] no engines created after processing entries.")
        else:
            print(f"[datasources] engines created: {created}; default={self.default_name!r}")

