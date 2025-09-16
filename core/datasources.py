from __future__ import annotations

"""Datasource registry with robust fallbacks.

This registry constructs SQLAlchemy engines from several possible sources in
priority order:

1. `DB_CONNECTIONS` setting for the active namespace
2. `APP_DB_URL`/`FA_DB_URL` setting for the namespace combined with
   `DEFAULT_DATASOURCE`
3. Environment variables `FA_DB_URL` or `APP_DB_URL`

If no engines are created after checking the above, a warning is printed and
any attempt to retrieve an engine will raise a ``RuntimeError``.
"""

import os
from sqlalchemy import create_engine


def _safe(v, default=None):
    return v if v not in (None, "", []) else default


class DatasourceRegistry:
    def __init__(self, settings, namespace: str | None = None):
        """
        Build engine map with multiple fallbacks.

        Parameters
        ----------
        settings:
            ``Settings`` instance providing configuration access.
        namespace:
            Namespace used when resolving settings; defaults to ``"default"``.
        """

        self.settings = settings
        self.namespace = namespace or "default"
        self._engines: dict[str, any] = {}
        self._default_name: str | None = None
        self._build()

    def _build(self) -> None:
        """Populate the internal engine map."""

        # 1) Settings: DB_CONNECTIONS scoped to namespace
        conns = (
            self.settings.get_json(
                "DB_CONNECTIONS", scope="namespace", namespace=self.namespace
            )
            or []
        )

        # 2) Fallback to APP_DB_URL/FA_DB_URL in settings
        if not conns:
            app_db_url = (
                self.settings.get_str(
                    "APP_DB_URL", scope="namespace", namespace=self.namespace
                )
                or self.settings.get_str(
                    "FA_DB_URL", scope="namespace", namespace=self.namespace
                )
            )
            if app_db_url:
                ds_name = self.settings.get_str(
                    "DEFAULT_DATASOURCE", scope="namespace", namespace=self.namespace
                ) or "docuware"
                conns = [{"name": ds_name, "url": app_db_url, "role": "oltp"}]

        # 3) Environment variable fallback
        if not conns:
            app_db_url = os.getenv("FA_DB_URL") or os.getenv("APP_DB_URL")
            if app_db_url:
                ds_name = self.settings.get_str(
                    "DEFAULT_DATASOURCE", scope="namespace", namespace=self.namespace
                ) or "docuware"
                conns = [{"name": ds_name, "url": app_db_url, "role": "oltp"}]

        for c in conns:
            url = _safe(c.get("url"))
            name = _safe(c.get("name"), "docuware")
            if not url:
                continue
            try:
                eng = create_engine(url, pool_pre_ping=True, pool_recycle=300)
                self._engines[name] = eng
                if not self._default_name:
                    self._default_name = name
            except Exception as e:  # pragma: no cover - logging only
                print(f"[datasources] failed to create engine '{name}': {e}")

        if not self._engines:
            print("[datasources] no engines created (check DB_CONNECTIONS or APP_DB_URL).")

    # ------------------------------------------------------------------
    def engine(self, name: str | None = None):
        """Return an engine by name or the configured default."""

        if name and name in self._engines:
            return self._engines[name]
        if self._default_name and self._default_name in self._engines:
            return self._engines[self._default_name]
        if len(self._engines) == 1:
            return next(iter(self._engines.values()))
        raise RuntimeError("No datasource engine found for requested datasource.")

