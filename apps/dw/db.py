"""Minimal DB helper used by the simplified DW blueprint."""

from __future__ import annotations

from typing import Any, Dict, List


def fetch_rows(sql: str, binds: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Return query results.

    This placeholder implementation simply returns an empty result set. Production
    deployments are expected to replace it with a real database call.
    """

    return []


__all__ = ["fetch_rows"]
