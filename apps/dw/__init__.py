from __future__ import annotations

from typing import Any

__all__ = ["dw_bp", "create_dw_blueprint", "NAMESPACE"]


def __getattr__(name: str) -> Any:  # pragma: no cover - simple lazy importer
    if name in __all__:
        from .app import NAMESPACE, create_dw_blueprint, dw_bp

        exports = {
            "dw_bp": dw_bp,
            "create_dw_blueprint": create_dw_blueprint,
            "NAMESPACE": NAMESPACE,
        }
        return exports[name]
    raise AttributeError(f"module 'apps.dw' has no attribute {name!r}")
