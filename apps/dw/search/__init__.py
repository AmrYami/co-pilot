"""Search helpers for DW endpoints."""

from typing import Callable, Dict, Iterable, List, Optional, Sequence, Tuple

from .filters import build_eq_where
from .fts import (
    build_boolean_groups_where,
    extract_fts_tokens,
    get_fts_columns,
    get_fts_engine,
    resolve_fts_config,
)
from .legacy import extract_search_tokens, inject_fulltext_where, is_fulltext_allowed
from .fts_registry import get_engine as _get_registered_engine, resolve_engine, register_engine
from ..settings_access import get_setting

try:  # pragma: no cover - optional dependency
    from apps.dw.fts_like import build_fts_where as _legacy_like_builder
except Exception:  # pragma: no cover - fallback when legacy helpers absent
    _legacy_like_builder = None


def _build_like_where(
    columns: Sequence[str],
    groups: Sequence[Sequence[str]],
    *,
    operator: str = "OR",
) -> Tuple[str, Dict[str, str]]:
    """Fallback LIKE-based builder used when no registry engine is found."""

    if _legacy_like_builder is None:
        return "", {}

    normalized_groups: List[List[str]] = []
    for group in groups or []:
        cleaned = [str(token or "").strip() for token in group or [] if str(token or "").strip()]
        if cleaned:
            normalized_groups.append(cleaned)
    if not normalized_groups:
        return "", {}

    try:
        return _legacy_like_builder(normalized_groups, columns, op_between_groups=operator)
    except TypeError:  # pragma: no cover - defensive
        return _legacy_like_builder(normalized_groups, columns)  # type: ignore[misc]


def _build_nofts_where(
    columns: Sequence[str],
    groups: Sequence[Sequence[str]],
    *,
    operator: str = "OR",
) -> Tuple[str, Dict[str, str]]:
    """Explicit no-FTS builder used when FTS is disabled."""

    return "", {}


try:  # pragma: no cover - optional engine
    from .oracle_text import build_oracle_text_where as _build_oracle_text_where
except Exception:  # pragma: no cover - oracle text may be unavailable in tests
    _build_oracle_text_where = None


ENGINE_MAP: Dict[str, Optional[Callable[[Sequence[str], Sequence[Sequence[str]], str], Tuple[str, Dict[str, str]]]]] = {
    "like": lambda columns, groups, operator="OR": _build_like_where(
        columns, groups, operator=operator
    ),
    "oracle_text": (
        (lambda columns, groups, operator="OR": _build_oracle_text_where(columns, groups, operator=operator))
        if _build_oracle_text_where
        else None
    ),
    "none": lambda columns, groups, operator="OR": _build_nofts_where(
        columns, groups, operator=operator
    ),
}


def get_engine(name: Optional[str]):
    """Resolve an FTS engine with a safe fallback to ``like``."""

    normalized = (name or "").strip().lower()

    builder = _get_registered_engine(normalized) if normalized else None
    if not builder:
        candidate = ENGINE_MAP.get(normalized)
        if candidate:
            builder = candidate

    if not builder:
        fallback = ENGINE_MAP.get("like")
        if fallback:
            builder = fallback

    return builder


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
    "ENGINE_MAP",
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
