from __future__ import annotations

from typing import Dict, List, Tuple


def build_fts_where(tokens: List[str], columns: List[str]) -> Tuple[str, Dict[str, str]]:
    normalised = [t.strip().lower() for t in tokens if t and t.strip()]
    if not normalised or not columns:
        return "", {}
    parts = []
    binds: Dict[str, str] = {}
    k = 0
    for token in normalised:
        ors = []
        for col in columns:
            param = f"kw{k}"
            ors.append(f"LOWER({col}) LIKE :{param}")
            binds[param] = f"%{token}%"
            k += 1
        parts.append("(" + " OR ".join(ors) + ")")
    where = " AND ".join(parts)
    return where, binds
