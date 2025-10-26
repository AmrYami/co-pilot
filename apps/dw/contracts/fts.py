# Utilities for Full-Text LIKE search over configured columns.
from __future__ import annotations
import re
from typing import List, Tuple, Dict
from apps.dw.lib.sql_utils import is_email, is_phone
try:
    # Reuse email/phone detectors to keep FTS tokens clean
    from apps.dw.rate_parser import is_email, is_phone
except Exception:  # pragma: no cover - defensive fallback
    import re as _re
    _EMAIL = _re.compile(r"(?i)\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b")
    _PHONE = _re.compile(r"\b\+?\d{7,15}\b")

    def is_email(s: str) -> bool:
        return bool(_EMAIL.search(s or ""))

    def is_phone(s: str) -> bool:
        return bool(_PHONE.search(s or ""))

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
            normalized = []
            for t in terms:
                token = _norm(t)
                if not token:
                    continue
                if is_email(token) or is_phone(token):
                    # push these to EQ filters elsewhere; exclude from FTS
                    continue
                normalized.append(token)
            if normalized:
                groups.append(normalized)
        return groups, "explicit"

    if force:
        # implicit mode: try quoted phrases, otherwise take long-ish tokens
        phrases = re.findall(r"'([^']+)'|\"([^\"]+)\"", q)
        terms = [t1 or t2 for (t1, t2) in phrases]
        if not terms:
            candidates = [w for w in re.findall(r"[A-Za-z][A-Za-z0-9_\- ]{2,}", q)]
            terms = [_norm(c) for c in candidates]
        cleaned = [t for t in terms if t and not is_email(t) and not is_phone(t)]
        return [[t] for t in cleaned], "implicit"

    return [], "none"

def build_fts_where_groups(
    groups: List[List[str]], columns: List[str], bind_prefix: str = "fts"
) -> Tuple[str, Dict[str, str]]:
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


# --- New helper APIs used by the planner --------------------------------------------------


def parse_fts_terms_from_question(q: str) -> List[str]:
    """Extract candidate FTS terms from the question following a ``has`` clause."""

    s = (q or "").strip().lower()
    if not s:
        return []
    match = re.search(r"\bhas\b(.*)$", s)
    tail = match.group(1) if match else s
    parts = re.split(r"\s*(?:or|and|,|/)\s*", tail)
    terms: List[str] = []
    for part in parts:
        token = part.strip(" ' \"")
        if token:
            terms.append(token)
    return terms


def normalize_terms(terms: List[str], short_allow: List[str]) -> List[str]:
    """Normalize, deduplicate, and cap the term list."""

    allow_set = {tok.upper() for tok in short_allow or []}
    filtered: List[str] = []
    for term in terms:
        upper = term.strip().upper()
        if not upper:
            continue
        if len(upper) < 3 and upper not in allow_set:
            continue
        filtered.append(term)

    seen: set[str] = set()
    dedup: List[str] = []
    for term in filtered:
        key = term.strip().upper()
        if not key or key in seen:
            continue
        seen.add(key)
        dedup.append(term)
    return dedup[:10]


def build_fts_where(
    table_cols: List[str], terms: List[str], binds: Dict[str, str]
) -> Tuple[str, Dict[str, str]]:
    """Construct a SQL WHERE fragment for the provided columns and terms."""

    if not table_cols or not terms:
        return "", binds

    clauses: List[str] = []
    for idx, term in enumerate(terms):
        bind = f"fts_t{idx}"
        binds[bind] = f"%{term}%"
        per_term = " OR ".join(
            [f"UPPER(TRIM({col})) LIKE UPPER(:{bind})" for col in table_cols]
        )
        clauses.append(f"({per_term})")

    if not clauses:
        return "", binds

    where_sql = "(" + " OR ".join(clauses) + ")"
    return where_sql, binds


def build_tokens(groups: List[List[str]]) -> List[List[str]]:
    """Drop email/phone-like and explicit 'col = val' patterns from token groups."""
    cleaned: List[List[str]] = []
    for g in groups or []:
        g2: List[str] = []
        for t in g or []:
            s = (t or "").strip()
            if not s:
                continue
            if is_email(s) or is_phone(s):
                continue
            if "=" in s:
                left, _, right = s.partition("=")
                if left.strip() and right.strip():
                    continue
            g2.append(s)
        if g2:
            cleaned.append(g2)
    return cleaned
