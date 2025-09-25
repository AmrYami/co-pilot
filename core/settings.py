from __future__ import annotations

import json
import os
import re
from typing import Any, Dict, List, Optional

from sqlalchemy import text


class Settings:
    """Lightweight accessor for namespace-scoped settings stored in mem_settings."""

    def __init__(self, namespace: str = "dw::common") -> None:
        self.namespace = namespace

    # ------------------------------------------------------------------
    def set_namespace(self, namespace: str) -> None:
        self.namespace = namespace

    # ------------------------------------------------------------------
    def _fetch(
        self,
        key: str,
        *,
        scope: str = "namespace",
        scope_id: Optional[str] = None,
        namespace: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        ns = namespace or self.namespace
        mem = self.mem_engine()
        if scope_id is None:
            stmt = text(
                """
                SELECT value, value_type
                  FROM mem_settings
                 WHERE namespace = :ns
                   AND key = :key
                   AND scope = :scope
                   AND scope_id IS NULL
                 ORDER BY updated_at DESC
                 LIMIT 1
                """
            )
            params = {"ns": ns, "key": key, "scope": scope}
        else:
            stmt = text(
                """
                SELECT value, value_type
                  FROM mem_settings
                 WHERE namespace = :ns
                   AND key = :key
                   AND scope = :scope
                   AND scope_id = :scope_id
                 ORDER BY updated_at DESC
                 LIMIT 1
                """
            )
            params = {"ns": ns, "key": key, "scope": scope, "scope_id": scope_id}

        with mem.connect() as conn:
            row = conn.execute(stmt, params).fetchone()
        if not row:
            return None
        return {"value": row[0], "value_type": row[1]}

    # ------------------------------------------------------------------
    def _coerce(self, value: Any, value_type: Optional[str]) -> Any:
        if value is None:
            return None
        if value_type is None:
            return value

        vtype = value_type.lower()
        if vtype in {"json", "jsonb"}:
            if isinstance(value, (dict, list)):
                return value
            try:
                return json.loads(value)
            except Exception:
                return value
        if vtype in {"bool", "boolean"}:
            if isinstance(value, bool):
                return value
            if isinstance(value, str):
                return value.strip().lower() in {"1", "true", "t", "yes", "y", "on"}
            return bool(value)
        if vtype in {"int", "integer"}:
            try:
                return int(value)
            except Exception:
                return None
        if vtype in {"float", "double", "numeric"}:
            try:
                return float(value)
            except Exception:
                return None
        return value

    # ------------------------------------------------------------------
    def get(
        self,
        key: str,
        default: Any = None,
        *,
        scope: str = "namespace",
        scope_id: Optional[str] = None,
        namespace: Optional[str] = None,
    ) -> Any:
        # The memory DB URL must be resolved without hitting mem_settings first
        if key == "MEMORY_DB_URL":
            env_val = os.getenv("MEMORY_DB_URL")
            if env_val:
                return env_val
            return default or "postgresql+psycopg2://postgres@localhost/copilot_mem_dev"

        rec = self._fetch(key, scope=scope, scope_id=scope_id, namespace=namespace)
        if rec:
            return self._coerce(rec["value"], rec.get("value_type"))

        env_val = os.getenv(key)
        if env_val is not None:
            return env_val
        return default

    # ------------------------------------------------------------------
    def get_string(
        self,
        key: str,
        default: Optional[str] = None,
        *,
        scope: str = "namespace",
        scope_id: Optional[str] = None,
        namespace: Optional[str] = None,
    ) -> Optional[str]:
        value = self.get(
            key,
            default=default,
            scope=scope,
            scope_id=scope_id,
            namespace=namespace,
        )
        if isinstance(value, str):
            return value
        if value is None:
            env_val = os.getenv(key)
            return env_val if env_val is not None else default
        if isinstance(value, (dict, list)):
            return json.dumps(value)
        return str(value)

    # ------------------------------------------------------------------
    def get_bool(
        self,
        key: str,
        default: Optional[bool] = None,
        *,
        scope: str = "namespace",
        scope_id: Optional[str] = None,
        namespace: Optional[str] = None,
    ) -> Optional[bool]:
        value = self.get(
            key,
            default=default,
            scope=scope,
            scope_id=scope_id,
            namespace=namespace,
        )
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "t", "yes", "y", "on"}
        if value is None:
            return default
        return bool(value)

    # ------------------------------------------------------------------
    def get_int(
        self,
        key: str,
        default: Optional[int] = None,
        *,
        scope: str = "namespace",
        scope_id: Optional[str] = None,
        namespace: Optional[str] = None,
    ) -> Optional[int]:
        value = self.get(
            key,
            default=default,
            scope=scope,
            scope_id=scope_id,
            namespace=namespace,
        )
        if value is None:
            return default
        if isinstance(value, int):
            return value
        try:
            return int(value)
        except Exception:
            return default

    # ------------------------------------------------------------------
    def get_json(
        self,
        key: str,
        default: Any = None,
        *,
        scope: str = "namespace",
        scope_id: Optional[str] = None,
        namespace: Optional[str] = None,
    ) -> Any:
        value = self.get(
            key,
            default=None,
            scope=scope,
            scope_id=scope_id,
            namespace=namespace,
        )
        if value is None:
            return default
        if isinstance(value, (dict, list)):
            return value
        if isinstance(value, str):
            try:
                return json.loads(value)
            except Exception:
                return default
        return value

    _IDENT_RGX = re.compile(r"[^0-9A-Za-z_]")

    @classmethod
    def _sanitize_ident(cls, s: str) -> str:
        return cls._IDENT_RGX.sub("", str(s or "")).upper()

    def get_fts_columns(self, table_name: str) -> List[str]:
        """
        Resolve the list of columns to use for FTS for a given table name.
        Order of precedence:
          1. mem_settings DW_FTS_COLUMNS mapping {table: [cols], "*": [cols]}
          2. DW_FTS_COLUMNS environment variable (comma-separated)
        """

        mapping = self.get_json("DW_FTS_COLUMNS", default={}) or {}
        if not isinstance(mapping, dict):
            mapping = {}

        cols: Optional[List[str]] = None
        table_key = str(table_name or "")
        for key in (table_key, table_key.upper(), table_key.lower()):
            val = mapping.get(key)
            if isinstance(val, list):
                cols = val
                break

        if cols is None:
            wildcard = mapping.get("*")
            if isinstance(wildcard, list):
                cols = wildcard

        if not cols:
            raw = os.getenv("DW_FTS_COLUMNS", "")
            if raw:
                cols = [c.strip() for c in raw.split(",") if c.strip()]

        cols = cols or []
        return [self._sanitize_ident(col) for col in cols]

    # ------------------------------------------------------------------
    def get_str(
        self,
        key: str,
        *,
        scope: str = "namespace",
        scope_id: Optional[str] = None,
        namespace: Optional[str] = None,
        default: Optional[str] = None,
    ) -> Optional[str]:
        return self.get_string(
            key,
            default=default,
            scope=scope,
            scope_id=scope_id,
            namespace=namespace,
        )

    # ------------------------------------------------------------------
    def get_app_db_url(self, namespace: Optional[str] = None) -> Optional[str]:
        ns = namespace or self.namespace
        val = self.get_string("APP_DB_URL", scope="namespace", namespace=ns)
        if val:
            return val
        return self.get_string("APP_DB_URL", scope="global")

    # ------------------------------------------------------------------
    def default_datasource(self, namespace: Optional[str] = None) -> Optional[str]:
        ns = namespace or self.namespace
        val = self.get_string("DEFAULT_DATASOURCE", scope="namespace", namespace=ns)
        if val:
            return val
        return self.get_string("DEFAULT_DATASOURCE", scope="global")

    # ------------------------------------------------------------------
    def research_allowed(self, datasource: str, namespace: Optional[str] = None) -> bool:
        ns = namespace or self.namespace
        policy = self.get_json("RESEARCH_POLICY", scope="namespace", namespace=ns, default={})
        if isinstance(policy, dict) and datasource in policy:
            return bool(policy[datasource])
        return bool(
            self.get_bool("RESEARCH_MODE", scope="namespace", namespace=ns, default=False)
        )

    # ------------------------------------------------------------------
    def mem_engine(self):
        from core.sql_exec import get_mem_engine

        return get_mem_engine(self)
