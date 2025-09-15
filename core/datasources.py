from __future__ import annotations

from sqlalchemy import create_engine


class DatasourceRegistry:
    def __init__(self, settings, *, namespace: str):
        self.settings = settings
        self.namespace = namespace
        self._engines = {}

        conns = self.settings.get_json("DB_CONNECTIONS", scope="namespace", default=[]) or []
        for c in conns:
            name = c.get("name")
            url = c.get("url")
            if name and url:
                self._engines[name] = create_engine(url, pool_pre_ping=True)

        # fallback to APP_DB_URL if no named DS provided
        if not self._engines:
            app_url = self.settings.get("APP_DB_URL", scope="namespace")
            if app_url:
                self._engines["__app__"] = create_engine(app_url, pool_pre_ping=True)

        if not self._engines:
            print("[datasources] no engines created (check DB_CONNECTIONS or APP_DB_URL).")

        self._default = self.settings.get("DEFAULT_DATASOURCE", scope="namespace") or \
                        ("frontaccounting_bk" if "frontaccounting_bk" in self._engines else None) or \
                        ("__app__" if "__app__" in self._engines else None)

    def engine(self, name: str | None):
        key = name or self._default
        if key and key in self._engines:
            return self._engines[key]
        raise RuntimeError("No datasource engine found for requested datasource.")

