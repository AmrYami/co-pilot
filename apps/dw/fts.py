from __future__ import annotations

import re
from typing import Dict, List, Tuple

_STOP = {
    "and",
    "or",
    "of",
    "the",
    "a",
    "an",
    "by",
    "for",
    "to",
    "in",
    "on",
    "at",
    "last",
    "next",
}


def tokenize(q: str) -> List[str]:
    text = (q or "").lower()
    text = re.sub(r"[^0-9a-zA-Z\u0600-\u06FF\s]+", " ", text)
    toks = [w for w in text.split() if w and w not in _STOP]
    return toks[:6]


def build_oracle_fts(table: str, cols: List[str], toks: List[str]) -> Tuple[str, Dict[str, str]]:
    if not cols or not toks:
        return "", {}

    clauses: List[str] = []
    binds: Dict[str, str] = {}
    for idx, tok in enumerate(toks, start=1):
        bind_name = f"kw{idx}"
        binds[bind_name] = f"%{tok.upper()}%"
        ors = [f"UPPER(NVL({col}, '')) LIKE :{bind_name}" for col in cols]
        clauses.append("(" + " OR ".join(ors) + ")")

    return "(" + " AND ".join(clauses) + ")", binds
