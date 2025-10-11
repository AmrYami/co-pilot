"""Search helpers for DW endpoints."""

from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from .filters import build_eq_where
from .fts import (
    build_boolean_groups_where,
    extract_fts_tokens,
    get_fts_columns,
    get_fts_engine,
    resolve_fts_config,
)
from .legacy import extract_search_tokens, inject_fulltext_where, is_fulltext_allowed
from .fts_registry import get_engine, resolve_engine, register_engine
from ..settings_access import get_setting


def _clean_columns(columns: Iterable[object]) -> List[str]:
    result: List[str] = []
    for column in columns or []:
        text = str(column or "").strip()
        if text:
            result.append(text)
    return result


def _clean_groups(groups: Iterable[Iterable[object]]) -> List[List[str]]:
    cleaned: List[List[str]] = []
    for group in groups or []:
        tokens: List[str] = []
        for token in group or []:
            text = str(token or "").strip()
            if text:
                tokens.append(text)
        if tokens:
            cleaned.append(tokens)
    return cleaned


def build_fulltext_where(
    columns: Sequence[str],
    groups: Sequence[Sequence[str]],
    *,
    engine: Optional[str] = None,
    operator: str = "OR",
    default_engine: str = "like",
) -> Tuple[str, Dict[str, str], Optional[str]]:
    """Build an FTS predicate using the registered engine.

    Returns a tuple ``(sql, binds, error)``. ``error`` is ``"no_engine"`` when no
    engine could be resolved or ``"no_predicate"`` when the builder produced an empty
    predicate while groups were provided.
    """

    configured = (engine or get_setting("DW_FTS_ENGINE") or default_engine or "").strip()
    builder = get_engine(configured.lower()) if configured else None
    fallback_builder = None
    if not builder and default_engine:
        fallback_builder = get_engine(default_engine.lower())
        if fallback_builder:
            builder = fallback_builder
            configured = default_engine
    if not builder:
        return "", {}, "no_engine"

    normalized_columns = _clean_columns(columns)
    normalized_groups = _clean_groups(groups)
    if not normalized_columns or not normalized_groups:
        return "", {}, None

    try:
        where_sql, binds = builder(normalized_columns, normalized_groups, operator=operator)
    except TypeError:
        where_sql, binds = builder(normalized_columns, normalized_groups)  # type: ignore[misc]

    if not where_sql:
        return "", dict(binds or {}), "no_predicate"

    return where_sql, dict(binds or {}), None


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
