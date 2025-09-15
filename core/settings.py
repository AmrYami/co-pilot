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

import json, threading, os
from typing import Any, Dict, Optional
from sqlalchemy import text
from sqlalchemy.engine import Engine
from dotenv import load_dotenv

load_dotenv()


class Settings:
    def __init__(
        self, namespace: str = "default", mem_engine: Engine | None = None
    ) -> None:
        self._namespace = namespace
        self._mem_engine = mem_engine
        self._runtime_overrides: Dict[str, Any] = {}
        self._cache: Dict[str, Any] = {}
        self._lock = threading.RLock()

    # ---- internal fetch helpers ----
    def _fetch_row(
        self,
        key: str,
        *,
        namespace: str,
        scope: str = "namespace",
        scope_id: str | None = None,
    ) -> Optional[dict]:
        """Fetch a raw row from mem_settings with caching."""
        if not self._mem_engine:
            return None
        cache_key = self._cache_key(namespace, key, scope, scope_id)
        if cache_key in self._cache:
            return self._cache[cache_key]

        params = {"key": key, "ns": namespace, "sc": scope, "sid": scope_id}
        if scope_id is None:
            sql = text(
                """
                SELECT key, value, value_type
                  FROM mem_settings
                 WHERE namespace = :ns AND key = :key AND scope = :sc AND scope_id IS NULL
                 LIMIT 1
                """
            )
        else:
            sql = text(
                """
                SELECT key, value, value_type
                  FROM mem_settings
                 WHERE namespace = :ns AND key = :key AND scope = :sc AND scope_id = :sid
                 LIMIT 1
                """
            )

        with self._mem_engine.begin() as c:
            row = c.execute(sql, params).mappings().first()

        if row:
            d = dict(row)
            self._cache[cache_key] = d
            return d
        self._cache[cache_key] = None
        return None

    def _coerce(self, value: Any, value_type: Optional[str]) -> Any:
        """Coerce the raw DB value according to its declared value_type."""
        v = value
        if isinstance(v, str):
            v_str = v.strip()
            if (
                (v_str.startswith("{") and v_str.endswith("}"))
                or (v_str.startswith("[") and v_str.endswith("]"))
                or (v_str.startswith('"') and v_str.endswith('"'))
                or v_str in {"true", "false", "null"}
            ):
                try:
                    v = json.loads(v_str)
                except Exception:
                    pass
        t = (value_type or "").lower()
        if t in ("bool", "boolean"):
            if isinstance(v, bool):
                return v
            if isinstance(v, str):
                return v.strip().lower() in {"1", "true", "yes", "on"}
            if isinstance(v, (int, float)):
                return bool(v)
            return False
        if t in ("int", "integer"):
            try:
                return int(v)
            except Exception:
                return None
        if t == "json":
            if isinstance(v, (dict, list)):
                return v
            try:
                return json.loads(v)
            except Exception:
                return None
        if t in ("string", "str", ""):
            if v is None:
                return None
            if isinstance(v, (dict, list)):
                return json.dumps(v)
            return str(v)
        return v

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

    def get(
        self,
        key: str,
        default: Any | None = None,
        *,
        scope: str | None = None,
        scope_id: str | None = None,
        namespace: str | None = None,
    ) -> Any:
        """Get a setting by key with precedence runtime→DB→env→default."""
        with self._lock:
            ns = namespace or self._namespace
            if key in self._runtime_overrides:
                return self._runtime_overrides[key]

            row = self._fetch_row(key, namespace=ns, scope=scope or "namespace", scope_id=scope_id)
            if row is not None:
                return self._coerce(row.get("value"), row.get("value_type"))

            env_val = os.getenv(key)
            if env_val is not None:
                return env_val
            return default

    def get_json(self, key: str, *, scope="namespace", default=None):
        v = self.get(key, scope=scope)
        return v if v is not None else default

    def get_str(
        self,
        key: str,
        default: Optional[str] = None,
        *,
        scope: str = "namespace",
        scope_id: str | None = None,
        namespace: str | None = None,
    ) -> Optional[str]:
        row = self._fetch_row(key, namespace=namespace or self._namespace, scope=scope, scope_id=scope_id)
        if not row:
            return default
        val = self._coerce(row.get("value"), row.get("value_type"))
        if val is None:
            return default
        return str(val)


    def get_bool(self, key: str, *, scope="namespace", default=False) -> bool:
        v = self.get(key, scope=scope)
        if isinstance(v, bool):
            return v
        if isinstance(v, str):
            s = v.strip().lower()
            if s in ("true", "1", "yes", "y", "on"):
                return True
            if s in ("false", "0", "no", "n", "off"):
                return False
        return bool(v) if v is not None else default

    def get_int(self, key: str, *, scope="namespace", default=0) -> int:
        v = self.get(key, scope=scope)
        try:
            return int(v)
        except Exception:
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
    def _cache_key(
        self, namespace: str, key: str, scope: str | None, scope_id: str | None
    ) -> str:
        return f"{namespace}|{scope or '*'}|{scope_id or '*'}|{key}"

    # Convenience: canonical single-DB accessor with safe fallbacks
    def get_app_db_url(self, namespace: str | None = None) -> str | None:
        # Prefer DB-backed settings (mem_settings), else env. Canonical key:
        #   APP_DB_URL  (namespace scope)
        # Back-compat fallbacks:
        #   FA_DB_URL   (namespace/global)
        for key in ("APP_DB_URL", "FA_DB_URL"):
            v = self.get(key, namespace=namespace)
            if v:
                return v
        # As a last resort, environment variables:
        import os

        return os.getenv("APP_DB_URL") or os.getenv("FA_DB_URL")

    def db_connections(self, namespace: str | None = None) -> list[dict]:
        ns = namespace or self._namespace
        v = self.get("DB_CONNECTIONS", namespace=ns)
        if isinstance(v, list):
            return v
        return []

    def default_datasource(self, namespace: str | None = None) -> str | None:
        ns = namespace or self._namespace
        v = self.get("DEFAULT_DATASOURCE", namespace=ns)
        return v if isinstance(v, str) and v else None

    def is_inline_clarifier(self, namespace: str, email: str) -> bool:
        allowed = set(self.get("ADMINS_CAN_CLARIFY_IMMEDIATE", namespace=namespace) or [])
        return (email or "").lower() in {e.lower() for e in allowed}

    def snapshot(self, namespace: str) -> Dict[str, Any]:
        prev = self._namespace
        self.set_namespace(namespace)
        snap = self.summary(mask_secrets=False)
        self.set_namespace(prev)
        return snap

    def research_allowed(self, datasource: str, namespace: str | None = None) -> bool:
        ns = namespace or self._namespace
        policy = self.get("RESEARCH_POLICY", namespace=ns) or {}
        if isinstance(policy, str):
            try:
                import json

                policy = json.loads(policy)
            except Exception:
                policy = {}
        if isinstance(policy, dict) and datasource in policy:
            return bool(policy[datasource])
        return bool(
            self.get("RESEARCH_MODE", namespace=ns)
            or self.get("RESEARCH_MODE")
        )

    def memory_db_url(self) -> str:
        v = self.get("MEMORY_DB_URL")
        if isinstance(v, str) and v:
            return v
        import os

        return os.getenv(
            "MEMORY_DB_URL",
            "postgresql+psycopg2://postgres@localhost/copilot_mem_dev",
        )

    def admins_can_clarify_immediate(self, namespace: str | None = None) -> set[str]:
        ns = namespace or self._namespace
        v = self.get("ADMINS_CAN_CLARIFY_IMMEDIATE", namespace=ns) \
            or self.get("ADMINS_CAN_CLARIFY_IMMIDIAT", namespace=ns)
        return set(v) if isinstance(v, list) else set()

    def admin_can_clarify_immediate(
        self, email: str | None, namespace: str | None = None
    ) -> bool:
        if not email:
            return False
        emails = _as_list(
            self.get("ADMINS_CAN_CLARIFY_IMMEDIATE", namespace=namespace)
        )
        return any(email.strip().lower() == e.strip().lower() for e in emails)

    def enduser_can_clarify(self, namespace: str | None = None) -> bool:
        v = self.get("ENDUSER_CAN_CLARIFY", namespace=namespace)
        return str(v).strip().lower() in {"1", "true", "yes", "y"}

    def empty_result_autoretry(self, namespace: str | None = None) -> bool:
        return bool(
            self.get("EMPTY_RESULT_AUTORETRY", default=False, namespace=namespace)
        )

    def empty_result_window_days(self, namespace: str | None = None) -> int:
        try:
            return int(
                self.get(
                    "EMPTY_RESULT_AUTORETRY_DAYS", default=90, namespace=namespace
                )
            )
        except Exception:
            return 90

    def snippets_autosave(self, namespace: str | None = None) -> bool:
        return bool(
            self.get("SNIPPETS_AUTOSAVE", default=True, namespace=namespace)
        )

    def research_enabled(self, namespace: str | None = None) -> bool:
        """Namespace-aware research policy with sane fallbacks."""
        # 1) per-namespace toggle
        v_ns = self.get("RESEARCH_MODE", namespace=namespace)
        if v_ns is not None and str(v_ns).strip().lower() in {"0", "false", "no", "n"}:
            return False
        if v_ns is not None and str(v_ns).strip().lower() in {"1", "true", "yes", "y"}:
            return True

        # 2) policy object (e.g., {"frontaccounting_bk": true, "rms2": false, ...})
        try:
            import json

            pol_raw = self.get("RESEARCH_POLICY", namespace=namespace)
            if pol_raw:
                pol = json.loads(pol_raw) if isinstance(pol_raw, str) else pol_raw
                if isinstance(pol, dict):
                    # if a datasource is present in context you can check it upstream
                    # here we just honor the namespace-level policy objectively
                    # absence means "no decision"
                    pass
        except Exception:
            pass

        # 3) global default
        v_glob = self.get("RESEARCH_MODE")
        return str(v_glob).strip().lower() in {"1", "true", "yes", "y"}

    def get_admin_emails(self) -> list[str]:
        vals = (
            self.get("ADMINS_CAN_CLARIFY_IMMEDIATE")
            or self.get("ADMIN_EMAILS")
            or self.get("ALERTS_EMAILS")
        )
        if isinstance(vals, list):
            return vals
        return []

    def smtp_sanity(self) -> None:
        sec = (self.get("SMTP_SECURITY") or "").lower().strip()
        port = int(self.get("SMTP_PORT") or 0)
        if sec == "ssl" and port == 587:
            print(
                "WARNING: SMTP_SECURITY=ssl usually uses port 465; 587 is typically starttls."
            )


# ---- Typed setting helpers & keys ----

# Default app namespace
DEFAULT_APP = "fa"

# ----- New keys (names exactly as you requested) -----
KEY_DB_CONNECTIONS = "DB_CONNECTIONS"  # list[{name,url,role?,default?}]
KEY_DEFAULT_DATASOURCE = "DEFAULT_DATASOURCE"  # "frontaccounting_bk"
KEY_RESEARCH_POLICY = "RESEARCH_POLICY"  # {name: bool}
KEY_ACTIVE_APP = "ACTIVE_APP"  # "fa"
KEY_AUTH_EMAIL = "AUTH_EMAIL"
KEY_ADMIN_EMAILS = "ADMIN_EMAILS"  # list[str]
KEY_ADMINS_INLINE = "ADMINS_CAN_CLARIFY_IMMEDIATE"  # list[str]
KEY_ADMINS_INLINE_LEGACY = "ADMINS_CAN_CLARIFY_IMMIDIAT"  # legacy misspelling
KEY_SETTINGS_ADMIN_KEY_HASH = "SETTINGS_ADMIN_KEY_HASH"  # PBKDF2/bcrypt hash (not raw)
KEY_MEMORY_DB_URL = "MEMORY_DB_URL"  # mem store URL
KEY_FA_CATEGORY_MAP = "FA_CATEGORY_MAP"  # app-specific (read by FA)


def _json_get(settings, key: str, default):
    v = settings.get(key)
    if v is None or v == "":
        return default
    try:
        return json.loads(v) if isinstance(v, str) else v
    except Exception:
        return default


def get_db_connections(settings) -> list[dict]:
    """Return list of {name,url,role?,default?}. Fallback to env URLs on first boot."""
    arr = _json_get(settings, KEY_DB_CONNECTIONS, [])
    if arr:
        return arr
    # Bootstrap fallback (env) if settings not yet written
    fa = settings.get("FA_DB_URL")
    mem = settings.get("MEMORY_DB_URL")
    out = []
    if fa:
        out.append(
            {"name": "frontaccounting_bk", "url": fa, "role": "oltp", "default": True}
        )
    if mem:
        out.append({"name": "memory", "url": mem, "role": "mem"})
    return out


def get_default_datasource(settings) -> str | None:
    return settings.get(KEY_DEFAULT_DATASOURCE)


def get_research_policy(settings) -> dict[str, bool]:
    return _json_get(settings, KEY_RESEARCH_POLICY, {})


def get_admin_emails(settings) -> list[str]:
    return [
        e.lower().strip()
        for e in _json_get(settings, KEY_ADMIN_EMAILS, [])
        if isinstance(e, str)
    ]


def _as_list(v):
    if v is None:
        return []
    if isinstance(v, (list, tuple)):
        return list(v)
    # try JSON string -> list
    try:
        import json

        j = json.loads(v) if isinstance(v, str) else v
        if isinstance(j, list):
            return j
    except Exception:
        pass
    # comma-separated fallback
    if isinstance(v, str) and "," in v:
        return [p.strip() for p in v.split(",") if p.strip()]
    return [str(v)]


def get_inline_clarify_allowlist(settings) -> set[str]:
    """Lowercased email allowlist for inline clarifications."""
    raw = settings.get(KEY_ADMINS_INLINE) or settings.get(KEY_ADMINS_INLINE_LEGACY)
    return {e.lower() for e in _as_list(raw)}


def get_mem_store_url(settings) -> str | None:
    """Return MEMORY_DB_URL (mem store). Env remains bootstrap fallback to reach mem_settings itself."""
    return settings.get(KEY_MEMORY_DB_URL) or settings.get("MEMORY_DB_URL")


def get_active_app(settings) -> str:
    return settings.get(KEY_ACTIVE_APP) or "fa"
