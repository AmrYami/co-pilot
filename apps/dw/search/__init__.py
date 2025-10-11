"""Search helpers for DW endpoints."""

from .filters import build_eq_where
from .fts import (
    build_boolean_groups_where,
    build_fulltext_where,
    extract_fts_tokens,
    get_fts_columns,
    get_fts_engine,
    resolve_fts_config,
)
from .legacy import extract_search_tokens, inject_fulltext_where, is_fulltext_allowed
from .fts_registry import resolve_engine, register_engine

__all__ = [
    "build_boolean_groups_where",
    "build_eq_where",
    "build_fulltext_where",
    "extract_fts_tokens",
    "get_fts_columns",
    "get_fts_engine",
    "resolve_fts_config",
    "extract_search_tokens",
    "inject_fulltext_where",
    "is_fulltext_allowed",
    "resolve_engine",
    "register_engine",
]
