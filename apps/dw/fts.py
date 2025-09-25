from __future__ import annotations
import os, re
from typing import Tuple, Dict, Any, List

STOP = set(["the", "and", "of", "in", "for", "by", "to", "a", "an", "on", "at", "last", "month", "months", "next", "this"])


def _env_columns() -> List[str]:
    v = os.getenv("DW_FTS_COLUMNS") or ""
    cols = [c.strip() for c in v.split(",") if c.strip()]
    return cols


def _tokens(text: str) -> List[str]:
    raw = re.split(r"[^\w]+", text.lower())
    return [t for t in raw if t and t not in STOP and len(t) >= 3]


def build_fts_clause(
    question: str, columns: List[str] | None = None
) -> Tuple[str, Dict[str, Any], List[str], List[str]]:
    cols = columns or _env_columns()
    toks = _tokens(question)
    if not cols or not toks:
        return ("", {}, toks, cols)
    where_parts = []
    binds: Dict[str, Any] = {}
    for i, tok in enumerate(toks[:8]):
        bname = f"kw_{i}"
        binds[bname] = f"%{tok}%"
        col_or = [f"UPPER({c}) LIKE UPPER(:{bname})" for c in cols]
        where_parts.append("(" + " OR ".join(col_or) + ")")
    return ("(" + " AND ".join(where_parts) + ")", binds, toks, cols)
