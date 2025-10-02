"""Utilities for reading DW namespace settings with safe defaults."""
from __future__ import annotations

from typing import Any, Iterable, List


def get_namespace_json(db: Any, key: str, default: Any) -> Any:
    """Return a JSON-like configuration value for the given key.

    ``db`` can be a settings object with ``fetch_setting``/``get_json``/``get`` methods or a
    plain mapping already containing the namespace data. The function tolerates absent keys
    by returning ``default``.
    """

    if db is None:
        return default

    # 1) Explicit fetch_setting hook (preferred by caller when available)
    fetch = getattr(db, "fetch_setting", None)
    if callable(fetch):
        try:
            row = fetch(key, scope="namespace")
        except TypeError:
            row = fetch(key)
        if row and isinstance(row, dict) and "value" in row:
            value = row.get("value")
            if value is not None:
                return value

    # 2) get_json / get style accessors
    for attr in ("get_json", "get"):
        getter = getattr(db, attr, None)
        if callable(getter):
            for kwargs in (
                {"default": default, "scope": "namespace"},
                {"default": default},
                {"scope": "namespace"},
                {},
            ):
                try:
                    value = getter(key, **kwargs)
                except TypeError:
                    continue
                if value is not None:
                    return value

    # 3) Mapping-like objects
    if isinstance(db, dict):
        value = db.get(key, default)
        return value if value is not None else default

    return default


def _normalize_columns(raw: Iterable[Any]) -> List[str]:
    seen: set[str] = set()
    out: List[str] = []
    for col in raw:
        if not isinstance(col, str):
            continue
        norm = col.strip().strip('"')
        if not norm:
            continue
        up = norm.upper()
        if up not in seen:
            seen.add(up)
            out.append(up)
    return out


def get_fts_columns(db: Any, table: str) -> List[str]:
    """Return the configured FTS columns for ``table`` with sensible defaults."""

    cfg = get_namespace_json(db, "DW_FTS_COLUMNS", default={})
    table_key = (table or "").strip('"')
    columns: Iterable[Any] = []
    if isinstance(cfg, dict):
        columns = cfg.get(table_key) or cfg.get(table_key.upper()) or cfg.get("*") or []

    cols = _normalize_columns(columns)
    if not cols and isinstance(cfg, dict):
        # Try wildcard under quoted table name as well
        quoted = f'"{table_key}"'
        cols = _normalize_columns(cfg.get(quoted, []))

    if not cols and table_key == "Contract":
        cols = _normalize_columns(
            [
                "CONTRACT_SUBJECT",
                "CONTRACT_PURPOSE",
                "OWNER_DEPARTMENT",
                "DEPARTMENT_OUL",
                "CONTRACT_OWNER",
                "CONTRACT_STAKEHOLDER_1",
                "CONTRACT_STAKEHOLDER_2",
                "CONTRACT_STAKEHOLDER_3",
                "CONTRACT_STAKEHOLDER_4",
                "CONTRACT_STAKEHOLDER_5",
                "CONTRACT_STAKEHOLDER_6",
                "CONTRACT_STAKEHOLDER_7",
                "CONTRACT_STAKEHOLDER_8",
                "LEGAL_NAME_OF_THE_COMPANY",
                "ENTITY",
                "ENTITY_NO",
                "REQUEST_TYPE",
                "CONTRACT_STATUS",
                "REQUESTER",
                "CONTRACT_ID",
            ]
        )

    return cols


def get_short_token_allow(db: Any) -> List[str]:
    """Return allow-list of short tokens (<=2 chars) permitted in FTS."""

    allow = get_namespace_json(db, "DW_FTS_SHORT_TOKENS_ALLOW", default=["IT", "HR", "QA"])
    return [str(item).strip().upper() for item in allow if isinstance(item, str) and item.strip()]


__all__ = ["get_namespace_json", "get_fts_columns", "get_short_token_allow"]
