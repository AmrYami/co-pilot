# -*- coding: utf-8 -*-
"""LIKE-based FTS fallback helpers for DocuWare planner."""
from __future__ import annotations

from typing import Dict, Iterable, List, Tuple

__all__ = ["build_fts_like_where_and_binds"]


def _normalize_columns(columns: Iterable[str]) -> List[str]:
    seen: set[str] = set()
    normalized: List[str] = []
    for raw in columns or []:
        if raw is None:
            continue
        text = str(raw).strip()
        if not text:
            continue
        if text.startswith('"') and text.endswith('"'):
            text = text[1:-1]
        upper = text.upper()
        if upper not in seen:
            seen.add(upper)
            normalized.append(upper)
    return normalized


def build_fts_like_where_and_binds(
    token_groups: List[List[str]],
    columns: Iterable[str],
    operator_between_groups: str = "OR",
) -> Tuple[str, Dict[str, str]]:
    """Construct a LIKE-based predicate across ``columns`` for ``token_groups``.

    Each ``token_groups`` entry represents tokens that must all match (AND). Groups
    themselves are joined using ``operator_between_groups``.
    """

    safe_columns = _normalize_columns(columns)
    if not safe_columns:
        return "", {}

    binds: Dict[str, str] = {}
    group_clauses: List[str] = []
    bind_index = 0

    for group in token_groups or []:
        if not group:
            continue
        token_predicates: List[str] = []
        for token in group:
            cleaned = str(token or "").strip()
            if not cleaned:
                continue
            bind_name = f"fts_{bind_index}"
            bind_index += 1
            binds[bind_name] = f"%{cleaned}%"
            any_column = " OR ".join(
                [f"UPPER(NVL({col},'')) LIKE UPPER(:{bind_name})" for col in safe_columns]
            )
            token_predicates.append(f"({any_column})")
        if token_predicates:
            group_clauses.append("(" + " AND ".join(token_predicates) + ")")

    if not group_clauses:
        return "", {}

    joiner = " OR " if (operator_between_groups or "").upper() != "AND" else " AND "
    where_sql = joiner.join(group_clauses)
    return where_sql, binds
