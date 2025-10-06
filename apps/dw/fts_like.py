from __future__ import annotations

from typing import Dict, List, Tuple


def _normalize_token(tok: str) -> str:
    """Trim whitespace and strip surrounding quotes from a token."""
    tok = (tok or "").strip()
    if (tok.startswith("'") and tok.endswith("'")) or (
        tok.startswith("\"") and tok.endswith("\"")
    ):
        tok = tok[1:-1].strip()
    return tok


def _like_group(columns: List[str], bind_name: str) -> str:
    """Build an OR group of LIKE clauses across the provided columns."""
    ors: List[str] = []
    for col in columns:
        ors.append(f"UPPER(NVL({col},'')) LIKE UPPER(:{bind_name})")
    return "(" + " OR ".join(ors) + ")"


def build_fts_like_where(
    tokens: List[str],
    columns: List[str],
    operator: str = "OR",
) -> Tuple[str, Dict[str, str]]:
    """Return a LIKE-based WHERE fragment and binds for FTS tokens."""
    tokens = [t for t in (tokens or []) if str(t or "").strip()]
    if not tokens or not columns:
        return "", {}

    groups: List[str] = []
    binds: Dict[str, str] = {}
    for idx, raw_token in enumerate(tokens):
        token = _normalize_token(raw_token)
        if not token:
            continue
        bind_name = f"fts_{idx}"
        binds[bind_name] = f"%{token}%"
        groups.append(_like_group(columns, bind_name))

    if not groups:
        return "", {}
    glue = " AND " if (operator or "").upper() == "AND" else " OR "
    return "(" + glue.join(groups) + ")", binds
