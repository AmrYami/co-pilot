import re
from typing import Dict, List, Tuple


def _tok(s: str) -> str:
    return (s or "").strip()


def parse_from_question(q: str, allowed_cols: List[str]) -> List[Tuple[str, List[str]]]:
    """
    Extract inline equality filters from a natural question, supporting
    multi-value OR for the same column.

      COL = v1 or v2 or v3  =>  [ (COL, [v1, v2, v3]) ]

    Stops at 'and' or when a new column assignment begins.
    Returns list of (COL, [values]) pairs using the original column casing from allowed_cols.
    """
    text = q or ""
    cols_ci = {str(c).upper(): str(c) for c in (allowed_cols or [])}
    eq_filters: List[Tuple[str, List[str]]] = []
    # pattern: <COL> = <VAL>( or <VAL> )* (until 'and' or a new assignment)
    pat = re.compile(r"(?i)\b([A-Z0-9_ ]+?)\s*=\s*([^\s][^;]*?)\b(?=(?:\s+and\b|\s+[A-Z0-9_ ]+\s*=|$))")
    for m in pat.finditer(text):
        col_u = _tok(m.group(1)).upper().replace(" ", "_")
        if col_u not in cols_ci:
            continue
        rhs = m.group(2)
        parts = re.split(r"(?i)\s+or\s+|\s*\|\s*", rhs)
        vals = [_tok(p) for p in parts if _tok(p)]
        if vals:
            eq_filters.append((cols_ci[col_u], vals))
    return eq_filters

