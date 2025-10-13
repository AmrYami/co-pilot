"""Lightweight data access helpers shared between DW endpoints."""

from __future__ import annotations

from typing import Any, Dict

try:  # pragma: no cover - optional dependency in tests
    from core.inquiries import fetch_inquiry  # type: ignore
except Exception:  # pragma: no cover - allow usage without full core stack
    fetch_inquiry = None  # type: ignore[assignment]

from core.settings import Settings


def _get_mem_engine():
    settings = Settings(namespace="dw::common")
    try:
        return settings.mem_engine()
    except Exception:  # pragma: no cover - fallback for environments without DB
        return None


def load_inquiry(inquiry_id: int | None) -> Dict[str, Any]:
    """Return the inquiry payload recorded in ``mem_inquiries`` when available."""

    if not inquiry_id:
        return {}

    if not callable(fetch_inquiry):
        return {}

    mem_engine = _get_mem_engine()
    if mem_engine is None:
        return {}

    try:
        row = fetch_inquiry(mem_engine, int(inquiry_id))
    except Exception:  # pragma: no cover - defensive fallback
        return {}

    return dict(row) if isinstance(row, dict) else row or {}


__all__ = ["load_inquiry"]
