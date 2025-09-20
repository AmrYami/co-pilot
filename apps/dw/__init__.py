# apps/dw/__init__.py
# Keep this minimal to avoid import-time errors and circular deps.
from .app import create_dw_blueprint

__all__ = ["create_dw_blueprint"]
