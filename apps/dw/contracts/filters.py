from __future__ import annotations

import re
from typing import Dict, List, Tuple

from .semantics import expand_status

# Regex for explicit "column = value" patterns in natural language
_EQ_PATTERNS = [
    re.compile(r"\bwhere\s+([A-Z_][A-Z0-9_]*)\s*=\s*([^\s,;]+)", re.I),
    re.compile(r"\b([A-Z_][A-Z0-9_]*)\s*=\s*([^\s,;]+)", re.I),
    re.compile(r"\b([A-Z_][A-Z0-9_]*)\s+(?:equals|is)\s*([^\s,;]+)", re.I),
]


def parse_explicit_filters(
    question: str,
    allowed_columns: List[str],
) -> Tuple[List[str], Dict[str, object]]:
    """
    Extract explicit column=value constraints. Return SQL snippets and bind dict.
    """
    q = (question or "").strip()
    if not q:
        return [], {}

    colset = {c.upper().strip(): c for c in (allowed_columns or [])}
    snippets: List[str] = []
    binds: Dict[str, object] = {}
    bind_idx = 0

    def next_bind_name() -> str:
        nonlocal bind_idx
        name = f"p{bind_idx}"
        bind_idx += 1
        return name

    for pattern in _EQ_PATTERNS:
        for match in pattern.finditer(q):
            col_raw, val_raw = match.group(1), match.group(2)
            col_key = col_raw.upper()
            if col_key not in colset:
                continue
            col = colset[col_key]
            value = val_raw.strip().strip('"').strip("'")

            if col_key == "CONTRACT_STATUS":
                alts = expand_status(value)
                bind_names = []
                for alt in alts:
                    bind_name = next_bind_name()
                    bind_names.append(f":{bind_name}")
                    binds[bind_name] = alt
                snippets.append(
                    "UPPER(TRIM(CONTRACT_STATUS)) IN (" + ", ".join(bind_names) + ")"
                )
            else:
                bind_name = next_bind_name()
                binds[bind_name] = value
                snippets.append(
                    f"UPPER(TRIM({col})) = UPPER(TRIM(:{bind_name}))"
                )

    return snippets, binds
