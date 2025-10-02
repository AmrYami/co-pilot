from __future__ import annotations

"""Settings helpers for DW planner utilities."""

from typing import Any, Dict, Iterable, List, Optional

try:  # pragma: no cover - optional import for environments providing direct accessor
    from apps.core.settings import get_setting  # type: ignore
except ImportError:  # pragma: no cover - fall back to project-level helper
    from apps.settings import get_setting_json as _get_setting_json  # type: ignore

    def get_setting(key: str, *, scope: str | None = None, namespace: str | None = None, default: Any | None = None) -> Any:
        target_namespace = namespace or "dw::common"
        try:
            return _get_setting_json(target_namespace, key, default)
        except TypeError:
            # Older helper signature without default parameter
            return _get_setting_json(target_namespace, key)


def _dedupe_columns(columns: Iterable[Any]) -> List[str]:
    seen: set[str] = set()
    result: List[str] = []
    for col in columns:
        if not isinstance(col, str):
            continue
        cleaned = col.strip()
        if not cleaned:
            continue
        key = cleaned.strip('"').upper()
        if key in seen:
            continue
        seen.add(key)
        result.append(cleaned)
    return result


def _coerce_cfg(config: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not isinstance(config, dict):
        return {}
    # Allow passing either the raw map (table -> columns) or full settings dict.
    if any(key in config for key in ("*", "Contract")):
        return config
    candidate = config.get("DW_FTS_COLUMNS")
    if isinstance(candidate, dict):
        return candidate
    return {}


def get_fts_columns_for(table_name: str, *, config: Optional[Dict[str, Any]] = None) -> List[str]:
    """Return configured FTS column list for ``table_name`` with case-insensitive lookup.

    ``config`` may be provided to bypass the global settings fetch. It can either be the
    direct ``DW_FTS_COLUMNS`` mapping or the parent settings dictionary containing that
    mapping. When omitted, the helper falls back to ``get_setting`` which is expected to
    read from the active namespace (``dw::common`` by default).
    """

    table_key = (table_name or "").strip()
    if not table_key:
        return []

    cfg = _coerce_cfg(config)
    if not cfg:
        raw = get_setting("DW_FTS_COLUMNS", scope="namespace")
        if isinstance(raw, dict):
            cfg = raw
        else:
            cfg = {}

    if not cfg:
        return []

    search_keys = [table_key, table_key.upper(), table_key.lower(), table_key.strip('"')]
    candidates: List[str] = []
    for key in search_keys:
        cols = cfg.get(key)
        if isinstance(cols, list) and cols:
            candidates = cols
            break
    if not candidates:
        wildcard = cfg.get("*")
        if isinstance(wildcard, list):
            candidates = wildcard

    return _dedupe_columns(candidates)


__all__ = ["get_fts_columns_for"]
