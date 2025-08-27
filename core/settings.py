"""
core/settings.py — single source of truth for configuration

Resolution order per key:
  1) runtime overrides (request-scoped)
  2) DB (mem_settings) for the active namespace and scope
  3) environment variables
  4) provided default (argument)

The DB is optional; if `mem_engine` is None, DB lookups are skipped.
All values are returned as strings unless the DB `value` is JSON; we then
return the native Python type from JSON.

Usage:
    from core.settings import Settings
    s = Settings(namespace="fa::2_", mem_engine=pg_engine)
    db_url = s.get("FA_DB_URL")

Thread-safety: Settings is lightweight; instantiate per request or
store one per process and call `set_namespace(...)` before use.
"""
from __future__ import annotations

import json, threading
from typing import Any, Dict, Optional
from sqlalchemy import text
from sqlalchemy.engine import Engine
from dotenv import load_dotenv
load_dotenv()

class Settings:
    def __init__(self, namespace: str = "default", mem_engine: Engine | None = None) -> None:
        self._namespace = namespace
        self._mem_engine = mem_engine
        self._runtime_overrides: Dict[str, Any] = {}
        self._cache: Dict[str, Any] = {}
        self._lock = threading.RLock()

    # ---------------- Public API ----------------
    def set_namespace(self, namespace: str) -> None:
        with self._lock:
            if namespace != self._namespace:
                self._namespace = namespace
                self._cache.clear()

    def attach_mem_engine(self, mem_engine: Engine) -> None:
        with self._lock:
            self._mem_engine = mem_engine
            self._cache.clear()

    def override_temp(self, key: str, value: Any) -> None:
        with self._lock:
            self._runtime_overrides[key] = value

    def clear_overrides(self) -> None:
        with self._lock:
            self._runtime_overrides.clear()

    def get(self, key: str, default: Any | None = None, *, scope: str | None = None, scope_id: str | None = None) -> Any:
        """Get a setting by key with precedence runtime→DB→env→default.
        `scope` can be 'user' with a `scope_id` to fetch user-specific overrides.
        """
        with self._lock:
            # runtime
            if key in self._runtime_overrides:
                return self._runtime_overrides[key]

            # DB
            val = self._get_from_db(key, scope=scope, scope_id=scope_id)
            if val is not None:
                return val

            # env
            from os import getenv
            env_val = getenv(key)
            if env_val is not None:
                return env_val

            return default

    def summary(self, mask_secrets: bool = True) -> Dict[str, Any]:
        """Return a snapshot of cached DB/env values for diagnostics."""
        snap: Dict[str, Any] = {}
        for k, v in self._cache.items():
            if isinstance(v, dict) and mask_secrets and v.get("is_secret"):
                snap[k] = "***"
            else:
                snap[k] = v.get("value") if isinstance(v, dict) and "value" in v else v
        return snap

    # ---------------- Internals ----------------
    def _get_from_db(self, key: str, *, scope: str | None, scope_id: str | None) -> Any | None:
        if not self._mem_engine:
            return None
        cache_key = self._cache_key(key, scope, scope_id)
        if cache_key in self._cache:
            entry = self._cache[cache_key]
            return entry["value"] if isinstance(entry, dict) else entry

        # precedence: user(scope=user+scope_id) > namespace > global
        sql = text(
            """
            SELECT key, value, value_type, is_secret, scope, scope_id
            FROM mem_settings
            WHERE key = :key AND (
                (scope = 'user' AND scope_id = :scope_id AND namespace = :ns) OR
                (scope = 'namespace' AND namespace = :ns) OR
                (scope = 'global')
            )
            ORDER BY CASE scope WHEN 'user' THEN 1 WHEN 'namespace' THEN 2 ELSE 3 END
            LIMIT 1
            """
        )
        params = {"key": key, "ns": self._namespace, "scope_id": scope_id}
        try:
            with self._mem_engine.connect() as c:  # type: ignore[union-attr]
                r = c.execute(sql, params).mappings().first()
        except Exception:
            r = None
        if not r:
            self._cache[cache_key] = None
            return None

        raw = r["value"]
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", "ignore")
        try:
            # value is JSONB in DB; convert to python
            parsed = raw if isinstance(raw, (dict, list, int, float, bool)) else json.loads(raw)
        except Exception:
            parsed = raw

        entry = {"value": parsed, "value_type": r["value_type"], "is_secret": r["is_secret"], "scope": r["scope"], "scope_id": r["scope_id"]}
        self._cache[cache_key] = entry
        return entry["value"]

    def _cache_key(self, key: str, scope: str | None, scope_id: str | None) -> str:
        return f"{self._namespace}|{scope or '*'}|{scope_id or '*'}|{key}"
