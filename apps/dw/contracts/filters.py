from __future__ import annotations

import re
from typing import Dict, List, Tuple

from .aliases import canonicalize_column
from .semantics import expand_status
from .synonyms import expand_enum_predicate

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


# Matches: "... where <col> = <value>"
EQUALS_RE = re.compile(
    r"\bwhere\s+([A-Za-z_ \-]+?)\s*=\s*['\"]?([A-Za-z0-9 _\-\/]+)['\"]?",
    re.IGNORECASE,
)


def try_parse_simple_equals(
    question: str, table: str, get_setting
) -> Tuple[str | None, Dict[str, object]]:
    """
    Try to parse patterns like:
      'Show contracts where REQUEST TYPE = Renewal'
    Returns (sql_where_fragment, binds) or (None, {}).
    """

    match = EQUALS_RE.search(question or "")
    if not match:
        return None, {}

    raw_col = match.group(1) or ""
    raw_val = match.group(2) or ""
    col = canonicalize_column(raw_col) or raw_col.strip().upper().replace(" ", "_")

    enum_cfg = (get_setting("DW_ENUM_SYNONYMS", {}) or {}) if callable(get_setting) else {}
    key = f"{table}.{col}"
    if key in enum_cfg:
        frag, binds = expand_enum_predicate(table, col, raw_val, get_setting)
        return frag, binds

    return f"UPPER({col}) = UPPER(:v_eq)", {"v_eq": raw_val}
