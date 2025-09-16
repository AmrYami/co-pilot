from core.settings import Settings
from core.sql_exec import get_engine_for_url


class DatasourceRegistry:
    def __init__(self, settings: Settings, namespace: str):
        self.settings = settings
        self.namespace = namespace
        self._engines: dict[str, any] = {}

        conns = settings.get_json("DB_CONNECTIONS", scope="namespace", default=[])
        if conns:
            for c in conns:
                name = c["name"]
                url = c["url"]
                self._engines[name] = get_engine_for_url(url)
        else:
            app_url = settings.get("APP_DB_URL", scope="namespace")
            if app_url:
                self._engines["__default__"] = get_engine_for_url(app_url)

        if not self._engines:
            print("[datasources] no engines created (check DB_CONNECTIONS or APP_DB_URL).")

    def engine(self, ds_name: str | None):
        if ds_name and ds_name in self._engines:
            return self._engines[ds_name]
        default_name = self.settings.get("DEFAULT_DATASOURCE", scope="namespace")
        if default_name and default_name in self._engines:
            return self._engines[default_name]
        if "__default__" in self._engines:
            return self._engines["__default__"]
        raise RuntimeError("No datasource engine found for requested datasource.")
