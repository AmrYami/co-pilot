from __future__ import annotations

import re
from typing import Dict, List, Tuple

_STOPWORDS = {
    "the",
    "a",
    "an",
    "of",
    "and",
    "by",
    "per",
    "for",
    "to",
    "in",
    "on",
    "at",
    "with",
    "contract",
    "contracts",
    "value",
    "gross",
    "net",
    "top",
    "last",
    "month",
    "months",
    "this",
    "next",
    "previous",
    "owner",
    "department",
    "requested",
    "start",
    "end",
    "date",
}

_WORD = re.compile(r"[A-Za-z0-9_]+")


def tokenize(text: str) -> List[str]:
    tokens = [t.lower() for t in _WORD.findall(text or "")]
    tokens = [t for t in tokens if t not in _STOPWORDS and len(t) >= 2]
    return tokens[:8]


def sanitize_columns(cols: List[str]) -> List[str]:
    return [re.sub(r"[^0-9A-Za-z_]", "", c).upper() for c in (cols or [])]


def build_oracle_fts_predicate(
    tokens: List[str],
    columns: List[str],
    bind_prefix: str = "fts",
    tokens_mode: str = "all",
) -> Tuple[str, Dict[str, str]]:
    """
    Build a case-insensitive LIKE predicate for Oracle FTS emulation.

    For each token we create an OR clause across all provided columns. The
    clauses are then combined using AND (default) or OR when
    ``tokens_mode == 'any'``. The function returns a tuple of the SQL fragment
    and the dictionary of bind parameters.
    """

    columns = sanitize_columns(columns)
    binds: Dict[str, str] = {}
    if not tokens or not columns:
        return "", binds

    per_token_clauses: List[str] = []
    for i, tok in enumerate(tokens):
        bind_name = f"{bind_prefix}{i}"
        binds[bind_name] = f"%{tok}%"
        ors = [f"UPPER({col}) LIKE UPPER(:{bind_name})" for col in columns]
        per_token_clauses.append("(" + " OR ".join(ors) + ")")

    glue = " AND " if tokens_mode.lower() != "any" else " OR "
    return "(" + glue.join(per_token_clauses) + ")", binds
