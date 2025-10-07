# -*- coding: utf-8 -*-
"""
Utilities to read DW settings in a consistent way.
All comments must stay in English (per project convention).
"""
from __future__ import annotations

try:
    from apps.common.settings import get_setting  # type: ignore
except Exception:  # pragma: no cover - fallback to DW helper
    from apps.dw.settings_util import get_setting  # type: ignore


def get_json_setting(ns_key: str, default=None):
    val = get_setting(ns_key)
    return val if isinstance(val, dict) or isinstance(val, list) else (default if default is not None else {})


def get_fts_engine() -> str:
    # allowed: "like" (safe), "oracle-text" (if enabled). Default to "like".
    eng = get_setting("DW_FTS_ENGINE")
    return str(eng).strip().lower() if eng else "like"


def get_fts_columns(table: str = "Contract") -> list:
    cols = get_json_setting("DW_FTS_COLUMNS", {})
    if isinstance(cols, dict):
        table_cols = cols.get(table) or []
        fallback_cols = cols.get("*") or []
        # merge and keep order: table-specific first, then any extras in "*"
        merged = list(dict.fromkeys([*table_cols, *fallback_cols]))
        return merged
    return []


def get_explicit_filter_columns() -> list:
    cols = get_setting("DW_EXPLICIT_FILTER_COLUMNS")
    return cols if isinstance(cols, list) else []


def get_enum_synonyms() -> dict:
    return get_json_setting("DW_ENUM_SYNONYMS", {})
