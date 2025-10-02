from __future__ import annotations

"""Lightweight FTS token extraction and SQL helpers."""

from typing import Dict, List, Tuple


def extract_fts_tokens(question: str) -> List[str]:
    """Return ordered, deduplicated tokens from simple ``has`` patterns."""

    if not question:
        return []

    q = " ".join((question or "").strip().split()).lower()
    if not q or " has " not in q:
        return []

    tail = q.split(" has ", 1)[1]
    normalized = (
        tail.replace(" and ", " or ")
        .replace(",", " or ")
        .replace("/", " or ")
    )
    raw_terms = [part.strip() for part in normalized.split(" or ")]

    tokens: List[str] = []
    seen: set[str] = set()
    for term in raw_terms:
        if not term:
            continue
        cleaned = term.strip("'\"")
        cleaned = " ".join(cleaned.split())
        if not cleaned:
            continue
        key = cleaned.lower()
        if key in {"or", "and", "=", "=="}:
            continue
        if key not in seen:
            seen.add(key)
            tokens.append(cleaned)
    return tokens


def build_fts_where(
    table: str,
    columns: List[str],
    tokens: List[str],
    *,
    start_index: int = 0,
) -> Tuple[str, Dict[str, str]]:
    """Construct a SQL WHERE fragment for case-insensitive LIKE search across columns."""

    if not columns or not tokens:
        return "", {}

    clauses: List[str] = []
    binds: Dict[str, str] = {}

    def _quote(col: str) -> str:
        c = col.strip()
        if c.startswith('"') and c.endswith('"'):
            return c
        return f'"{c}"'

    quoted_cols = [_quote(col) for col in columns if col]
    for idx, token in enumerate(tokens):
        bind = f"fts_{start_index + idx}"
        binds[bind] = f"%{token}%"
        like_parts = [f"UPPER(TRIM({col})) LIKE UPPER(:{bind})" for col in quoted_cols]
        if like_parts:
            clauses.append("(" + " OR ".join(like_parts) + ")")

    if not clauses:
        return "", {}

    return "(" + " OR ".join(clauses) + ")", binds


__all__ = ["extract_fts_tokens", "build_fts_where"]
