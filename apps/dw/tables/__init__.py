from __future__ import annotations
from typing import Dict, TYPE_CHECKING

from .base import TableSpec

if TYPE_CHECKING:
    from core.settings import Settings

_REGISTRY: Dict[str, TableSpec] = {}


def register(spec: TableSpec) -> None:
    _REGISTRY[spec.name.lower()] = spec


def get(name: str) -> TableSpec:
    return _REGISTRY[name.lower()]


def for_namespace(settings: "Settings") -> TableSpec:
    """
    Pick the active table from settings (for DocuWare you used DW_CONTRACT_TABLE).
    Falls back to 'Contract' if not set.
    """
    tbl = (settings.get("DW_CONTRACT_TABLE") or "Contract")
    return get(str(tbl))


# Ensure default Contract spec is registered on import
from . import contract  # noqa: E402,F401
