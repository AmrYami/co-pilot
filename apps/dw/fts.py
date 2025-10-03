"""Full-text search helpers for DW queries."""

from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List, Optional, Tuple


_STOP_WORDS = {"list", "all", "contracts", "where", "has", "have"}


def extract_fts_tokens(question: str) -> List[str]:
    """Backward-compatible token extraction used by legacy planners."""

    tokens, _ = _tokenize_question(question or "")
    return tokens


def build_fts_where(
    question: str,
    settings: Any,
    *,
    table: str = "Contract",
    mode: str = "auto",
) -> Tuple[Optional[str], Dict[str, str], Optional[str]]:
    """Return a SQL WHERE fragment for LIKE-based FTS and the bind parameters."""

    columns = list(_load_fts_columns(settings, table))
    if not columns:
        return None, {}, None

    tokens, join_op = _tokenize_question(question or "")
    if not tokens:
        return None, {}, None

    clauses: List[str] = []
    binds: Dict[str, str] = {}

    for idx, token in enumerate(tokens):
        bind = f"fts_{idx}"
        binds[bind] = f"%{token}%"
        per_col = [f"UPPER(TRIM({col})) LIKE UPPER(:{bind})" for col in columns]
        if per_col:
            clauses.append("(" + " OR ".join(per_col) + ")")

    if not clauses:
        return None, {}, None

    where_sql = "(" + f" {join_op} ".join(clauses) + ")"
    return where_sql, binds, join_op


def _tokenize_question(question: str) -> Tuple[List[str], str]:
    text = " ".join((question or "").split()).lower()
    if not text:
        return [], "OR"

    if " and " in text:
        parts = re.split(r"\s+and\s+", text)
        op = "AND"
    elif " or " in text:
        parts = re.split(r"\s+or\s+", text)
        op = "OR"
    else:
        parts = [text]
        op = "OR"

    tokens: List[str] = []
    seen: set[str] = set()
    for part in parts:
        for segment in re.split(r"[,/;]\s*", part):
            words = [w for w in segment.split() if w and w not in _STOP_WORDS]
            if not words:
                continue
            phrase = " ".join(words)
            phrase = phrase.strip("'\"")
            if not phrase or phrase in seen:
                continue
            seen.add(phrase)
            tokens.append(phrase)
    return tokens, op


def _load_fts_columns(settings: Any, table: str) -> Iterable[str]:
    raw = _settings_get(settings, "DW_FTS_COLUMNS", {}) or {}
    columns: List[str] = []
    if isinstance(raw, dict):
        candidates = [table, table.strip('"'), table.upper(), table.lower(), "*"]
        for key in candidates:
            vals = raw.get(key)
            if isinstance(vals, list):
                columns.extend(vals)
    elif isinstance(raw, list):
        columns.extend(raw)

    for col in columns:
        norm = _normalize_column(col)
        if norm:
            yield norm


def _normalize_column(col: Any) -> Optional[str]:
    if not isinstance(col, str):
        return None
    cleaned = col.strip()
    if not cleaned:
        return None
    if cleaned.startswith('"') and cleaned.endswith('"'):
        return cleaned
    cleaned = cleaned.replace(" ", "_")
    if "." in cleaned:
        return ".".join(_normalize_column(part) or part for part in cleaned.split("."))
    return f'"{cleaned.upper()}"'


def _settings_get(settings: Any, key: str, default: Any = None) -> Any:
    if settings is None:
        return default
    getter_json = getattr(settings, "get_json", None)
    if callable(getter_json):
        try:
            value = getter_json(key, default)
        except TypeError:
            value = getter_json(key)
        if value is not None:
            return value
    getter = getattr(settings, "get", None)
    if callable(getter):
        try:
            value = getter(key, default)
        except TypeError:
            value = getter(key)
        if value is not None:
            return value
    if isinstance(settings, dict):
        return settings.get(key, default)
    return default


__all__ = ["extract_fts_tokens", "build_fts_where"]
