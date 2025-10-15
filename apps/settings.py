from __future__ import annotations

from typing import Any

from core.settings import Settings


_SETTINGS_SINGLETON = Settings()


def get_setting(
    key: str,
    default: Any | None = None,
    *,
    scope: str = "namespace",
    namespace: str = "dw::common",
    scope_id: str | None = None,
) -> Any:
    """Fetch a scalar setting value from ``mem_settings``.

    The helper mirrors the :class:`~core.settings.Settings` interface while providing a
    simple, reusable accessor for modules that only need the raw value. By keeping a
    module-level ``Settings`` singleton we avoid repeatedly instantiating the accessor
    while still honouring per-call overrides for ``namespace`` or ``scope``.
    """

    try:
        return _SETTINGS_SINGLETON.get(
            key,
            default=default,
            scope=scope,
            scope_id=scope_id,
            namespace=namespace,
        )
    except TypeError:
        # Older ``Settings`` helpers may not accept ``scope_id``/``namespace`` keyword
        # arguments; fall back to positional invocation for compatibility in tests.
        return _SETTINGS_SINGLETON.get(key, default)


def get_setting_json(namespace: str, key: str, default: Any | None = None) -> Any:
    """Fetch a JSON (or JSON-like) setting for the given namespace and key."""
    settings = Settings(namespace=namespace)
    try:
        return settings.get_json(key, default=default, scope="namespace")
    except Exception:
        return default


__all__ = ["get_setting", "get_setting_json"]
