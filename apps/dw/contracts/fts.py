# Utilities for Full-Text LIKE search over configured columns.
from __future__ import annotations
import re
from typing import List, Tuple, Dict

OR_SEP = re.compile(r"\s+(?:or|\|)\s+", flags=re.IGNORECASE)
AND_SEP = re.compile(r"\s+(?:and|&)\s+", flags=re.IGNORECASE)

def _norm(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()

def extract_fts_terms(question: str, force: bool = False) -> Tuple[List[List[str]], str]:
    """
    Extract FTS terms from a natural-language question.

    Returns (groups, mode) where:
      - groups: list of OR groups; each group is a list of terms combined with AND.
      - mode: "explicit" if we detected 'has ...', "implicit" if force=True, otherwise "none".

    Examples:
      "list contracts has it or home care"  -> [["it"], ["home care"]]
      "list contracts has home care and nursing" -> [["home care", "nursing"]]
    """
    q = question or ""
    ql = f" {q.lower()} "
    if " has " in ql:
        tail = _norm(ql.split(" has ", 1)[1])
        or_groups = [g for g in OR_SEP.split(tail) if g]
        groups: List[List[str]] = []
        for g in or_groups:
            terms = [t for t in AND_SEP.split(g) if t]
            groups.append([_norm(t) for t in terms])
        return groups, "explicit"

    if force:
        # implicit mode: try quoted phrases, otherwise take long-ish tokens
        phrases = re.findall(r"'([^']+)'|\"([^\"]+)\"", q)
        terms = [t1 or t2 for (t1, t2) in phrases]
        if not terms:
            candidates = [w for w in re.findall(r"[A-Za-z][A-Za-z0-9_\- ]{2,}", q)]
            terms = [_norm(c) for c in candidates]
        return [[t] for t in terms], "implicit"

    return [], "none"

def build_fts_where(groups: List[List[str]], columns: List[str], bind_prefix: str = "fts") -> Tuple[str, Dict[str, str]]:
    """
    Build a SQL WHERE predicate using UPPER(col) LIKE UPPER(:bind) across the provided columns.
    OR between groups; AND within a group; OR across columns for the same term.
    """
    binds: Dict[str, str] = {}
    if not groups or not columns:
        return "", binds

    group_sql = []
    bind_i = 0

    # normalize columns to be safe identifiers (assume they are already uppercase names)
    cols = [c if c.startswith('"') else f'"{c}"' for c in columns]

    for and_terms in groups:
        term_sql = []
        for term in and_terms:
            if not term:
                continue
            bname = f"{bind_prefix}_{bind_i}"
            binds[bname] = f"%{term}%"
            bind_i += 1
            col_sql = [f"UPPER({col}) LIKE UPPER(:{bname})" for col in cols]
            term_sql.append("(" + " OR ".join(col_sql) + ")")
        if term_sql:
            group_sql.append("(" + " AND ".join(term_sql) + ")")

    if not group_sql:
        return "", {}

    return "(" + " OR ".join(group_sql) + ")", binds
