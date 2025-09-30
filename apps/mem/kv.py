"""Helpers for retrieving namespace-scoped settings for deterministic planners."""

from __future__ import annotations

import importlib
from typing import Any, Dict, Optional

_flask_spec = importlib.util.find_spec("flask")
if _flask_spec is not None:  # pragma: no cover - import side effect
    from flask import current_app  # type: ignore
else:  # pragma: no cover - testing environments without Flask
    current_app = None  # type: ignore[assignment]


def _get_pipeline() -> Optional[object]:
    """Return the configured Pipeline object from the Flask app (if present)."""

    app = current_app
    if app is None:
        return None

    config = getattr(app, "config", {})
    getter = getattr(config, "get", None)
    if callable(getter):
        pipeline = getter("PIPELINE") or getter("pipeline")
    elif isinstance(config, dict):
        pipeline = config.get("PIPELINE") or config.get("pipeline")
    else:
        pipeline = None

    if pipeline is not None:
        return pipeline

    # Fallbacks for tests where pipeline may be attached differently
    return getattr(app, "pipeline", None)


def _extract_enum_synonyms(settings_obj: object, namespace: str) -> Dict[str, Any]:
    """Safely extract DW_ENUM_SYNONYMS from various settings interfaces."""

    enum_map: Dict[str, Any] = {}
    if settings_obj is None:
        return enum_map

    getter = getattr(settings_obj, "get_json", None)
    if callable(getter):
        try:
            value = getter("DW_ENUM_SYNONYMS", scope="namespace", namespace=namespace)
        except TypeError:
            value = getter("DW_ENUM_SYNONYMS")
        if isinstance(value, dict):
            enum_map = value
        return enum_map

    plain_get = getattr(settings_obj, "get", None)
    if callable(plain_get):
        try:
            value = plain_get("DW_ENUM_SYNONYMS", scope="namespace", namespace=namespace)
        except TypeError:
            value = plain_get("DW_ENUM_SYNONYMS")
        if isinstance(value, dict):
            enum_map = value
        return enum_map

    # Some Settings implementations expose a namespace-specific accessor
    ns_getter = getattr(settings_obj, "for_namespace", None)
    if callable(ns_getter):
        try:
            scoped = ns_getter(namespace)
        except Exception:
            scoped = None
        if scoped is not None:
            return _extract_enum_synonyms(scoped, namespace)

    return enum_map


def get_settings_for_namespace(namespace: str) -> Dict[str, Any]:
    """Return a lightweight dict of namespace settings required by planners."""

    pipeline = _get_pipeline()
    settings_obj = getattr(pipeline, "settings", None) if pipeline else None

    enum_map = _extract_enum_synonyms(settings_obj, namespace)

    return {"DW_ENUM_SYNONYMS": enum_map or {}}


__all__ = ["get_settings_for_namespace"]
