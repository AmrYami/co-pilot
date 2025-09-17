from __future__ import annotations

import json
import os
from typing import Any, Dict, Optional

from sqlalchemy import text

from core.sql_exec import get_mem_engine


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
        mem = get_mem_engine(self)
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
    def get_json(
        self,
        key: str,
        *,
        scope: str = "namespace",
        scope_id: Optional[str] = None,
        namespace: Optional[str] = None,
        default: Any = None,
    ) -> Any:
        rec = self._fetch(key, scope=scope, scope_id=scope_id, namespace=namespace)
        if rec is None or rec["value"] is None:
            return default
        val = rec["value"]
        if isinstance(val, (dict, list)):
            return val
        try:
            return json.loads(val)
        except Exception:
            return default if val is None else val

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
        rec = self._fetch(key, scope=scope, scope_id=scope_id, namespace=namespace)
        if rec is None or rec["value"] is None:
            env_val = os.getenv(key)
            return env_val if env_val is not None else default
        val = rec["value"]
        if isinstance(val, str):
            return val
        if isinstance(val, (dict, list)):
            return json.dumps(val)
        return str(val)

    # ------------------------------------------------------------------
    def get_bool(
        self,
        key: str,
        *,
        scope: str = "namespace",
        scope_id: Optional[str] = None,
        namespace: Optional[str] = None,
        default: bool = False,
    ) -> bool:
        rec = self._fetch(key, scope=scope, scope_id=scope_id, namespace=namespace)
        if rec is None or rec["value"] is None:
            env_val = os.getenv(key)
            if env_val is None:
                return default
            return env_val.strip().lower() in {"1", "true", "t", "yes", "y", "on"}
        val = self._coerce(rec["value"], rec.get("value_type"))
        if isinstance(val, bool):
            return val
        if isinstance(val, str):
            return val.strip().lower() in {"1", "true", "t", "yes", "y", "on"}
        return bool(val)

    # ------------------------------------------------------------------
    def get_app_db_url(self, namespace: Optional[str] = None) -> Optional[str]:
        ns = namespace or self.namespace
        val = self.get_str("APP_DB_URL", scope="namespace", namespace=ns)
        if val:
            return val
        return self.get_str("APP_DB_URL", scope="global")

    # ------------------------------------------------------------------
    def default_datasource(self, namespace: Optional[str] = None) -> Optional[str]:
        ns = namespace or self.namespace
        val = self.get_str("DEFAULT_DATASOURCE", scope="namespace", namespace=ns)
        if val:
            return val
        return self.get_str("DEFAULT_DATASOURCE", scope="global")

    # ------------------------------------------------------------------
    def research_allowed(self, datasource: str, namespace: Optional[str] = None) -> bool:
        ns = namespace or self.namespace
        policy = self.get_json("RESEARCH_POLICY", scope="namespace", namespace=ns, default={})
        if isinstance(policy, dict) and datasource in policy:
            return bool(policy[datasource])
        return self.get_bool("RESEARCH_MODE", scope="namespace", namespace=ns, default=False)
