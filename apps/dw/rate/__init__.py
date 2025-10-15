"""Rate blueprint package with optional helpers."""

from __future__ import annotations

from typing import Any

__all__ = ["rate_bp", "RateIntent", "parse_rate_comment", "build_sql"]


def __getattr__(name: str) -> Any:  # pragma: no cover - simple lazy import
    if name == "rate_bp":
        from .view import rate_bp

        return rate_bp
    if name in {"RateIntent", "parse_rate_comment", "build_sql"}:
        from .core import RateIntent, build_sql, parse_rate_comment

        mapping = {
            "RateIntent": RateIntent,
            "parse_rate_comment": parse_rate_comment,
            "build_sql": build_sql,
        }
        return mapping[name]
    raise AttributeError(f"module 'apps.dw.rate' has no attribute {name!r}")
