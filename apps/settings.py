from __future__ import annotations

from typing import Any

from core.settings import Settings


def get_setting_json(namespace: str, key: str, default: Any | None = None) -> Any:
    """Fetch a JSON (or JSON-like) setting for the given namespace and key."""
    settings = Settings(namespace=namespace)
    try:
        return settings.get_json(key, default=default, scope="namespace")
    except Exception:
        return default


__all__ = ["get_setting_json"]
