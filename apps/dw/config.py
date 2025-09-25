from __future__ import annotations

from typing import Any, List


def get_dw_fts_columns(settings: "Settings", table: str) -> List[str]:
    """Return the configured DW full-text search columns for ``table``."""

    mapping: Any = settings.get("DW_FTS_COLUMNS", scope="namespace") or {}
    if not isinstance(mapping, dict):
        return []

    def _sanitize(value: Any) -> List[str]:
        return [c for c in value if isinstance(c, str)]

    table_cols = mapping.get(table)
    if isinstance(table_cols, list):
        return _sanitize(table_cols)

    wildcard_cols = mapping.get("*")
    if isinstance(wildcard_cols, list):
        return _sanitize(wildcard_cols)
    return []
