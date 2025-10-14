"""Rate blueprint package with optional helpers."""

from __future__ import annotations

from typing import Any

__all__ = ["rate_bp"]


def __getattr__(name: str) -> Any:  # pragma: no cover - simple lazy import
    if name == "rate_bp":
        from .view import rate_bp

        return rate_bp
    raise AttributeError(f"module 'apps.dw.rate' has no attribute {name!r}")
