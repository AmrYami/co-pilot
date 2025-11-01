# Utilities for Full-Text LIKE search over configured columns.
from __future__ import annotations
import os
import re
from typing import List, Tuple, Dict
from apps.dw.lib.sql_utils import is_email, is_phone
try:
    from apps.dw.nlp import detect_alias_spans  # type: ignore
except ImportError:  # pragma: no cover - optional dependency
    detect_alias_spans = None  # type: ignore
_get_setting = None
try:  # pragma: no cover
    from apps.dw.settings import get_setting as _get_setting  # type: ignore
except Exception:  # pragma: no cover
    try:
        from apps.dw.settings_util import get_setting as _get_setting  # type: ignore
    except Exception:
        def _get_setting(*args, **kwargs):  # type: ignore
            return None
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


_COMPARATOR_MARKERS = [
    "greater than",
    "greater than or equal",
    "more than",
    "over",
    "above",
    "less than",
    "less than or equal",
    "under",
    "below",
    "at least",
    "at most",
    "no less than",
    "no more than",
    "between",
    ">=",
    "<=",
    ">",
    "<",
    "≥",
    "≤",
]


_NUMBER_WORDS = {
    "zero",
    "one",
    "two",
    "three",
    "four",
    "five",
    "six",
    "seven",
    "eight",
    "nine",
    "ten",
    "eleven",
    "twelve",
    "thirteen",
    "fourteen",
    "fifteen",
    "sixteen",
    "seventeen",
    "eighteen",
    "nineteen",
    "twenty",
    "thirty",
    "forty",
    "fifty",
    "sixty",
    "seventy",
    "eighty",
    "ninety",
    "hundred",
    "thousand",
    "million",
    "billion",
}


def _looks_like_numeric_clause(token: str) -> bool:
    if not token:
        return False
    lower = token.lower()
    if any(marker in lower for marker in _COMPARATOR_MARKERS):
        # ensure there is some numeric signal (digits or number words)
        if re.search(r"\d", lower):
            return True
        if any(word in lower for word in _NUMBER_WORDS):
            return True
    return False


def _eq_alias_keys() -> List[str]:
    """Return DW_EQ_ALIAS_COLUMNS keys (uppercased) to detect alias EQ tails.

    Be tolerant to different get_setting signatures (with/without scope/namespace).
    """
    aliases = {}
    getter = _get_setting
    try:
        if callable(getter):
            try:
                aliases = getter("DW_EQ_ALIAS_COLUMNS", scope="namespace", namespace="dw::common") or {}
            except TypeError:
                # Older signatures may not accept scope/namespace
                aliases = getter("DW_EQ_ALIAS_COLUMNS") or {}
    except Exception:
        aliases = {}
    if not isinstance(aliases, dict):
        aliases = {}
    keys: List[str] = []
    for k in aliases.keys():
        s = str(k or "").strip().upper()
        if s:
            keys.append(s)
    # Fallback defaults if settings missing
    if not keys:
        keys = [
            "DEPARTMENT", "DEPARTMENTS", "OWNER", "EMAIL", "STAKEHOLDER", "STAKEHOLDERS",
            "OUL", "REQUEST TYPE", "CONTRACT STATUS",
        ]
    return keys


def _cut_tail_at_alias(s: str, eq_aliases: List[str]) -> str:
    """Cut FTS tail at first 'AND|OR <ALIAS> (=|IN|LIKE|BETWEEN)' occurrence."""
    if not s or not eq_aliases:
        return s
    use_spacy = str(os.getenv("DW_USE_SPACY_ALIASES", "")).strip().lower() in {"1", "true", "t", "yes", "on"}
    if use_spacy and detect_alias_spans:
        try:
            spans = detect_alias_spans(s, eq_aliases)
        except Exception:
            spans = []
        if spans:
            for start, _, _ in spans:
                prefix = s[:start]
                if re.search(r"(?i)\b(and|or)\s+$", prefix):
                    return prefix.rstrip()
        # fall through to regex trim if no connector found
    alias_pat = r"(?:%s)" % "|".join(map(re.escape, eq_aliases))
    cut_rx = re.compile(rf"(?i)\b(?:and|or)\s+(?=({alias_pat})\b\s*(?:=|\bin\b|\blike\b|\bbetween\b))")
    parts = cut_rx.split(s, maxsplit=1)
    return parts[0] if parts else s

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
    # Treat these cues as explicit FTS triggers
    # Examples: "has/ have it or home care", "contain/contains maintenance",
    #           "including/included policy", "about it support", "mentioning warranty"
    cues = [
        " has ",
        " have ",
        " contain ",
        " contains ",
        " including ",
        " included ",
        " about ",
        " mentioning ",
    ]
    best_idx: int | None = None
    best_cue: str | None = None
    for cue in cues:
        idx = ql.find(cue)
        if idx == -1:
            continue
        if best_idx is None or idx < best_idx:
            best_idx = idx
            best_cue = cue
    if best_cue is not None:
        cue_trim = best_cue.strip()
        # Extract the original-case tail right after the earliest cue
        start = -1
        if best_idx is not None:
            # ql has a leading space, so adjust the index back into q.lower()
            approx_idx = max(best_idx - 1, 0)
            start = q.lower().find(cue_trim, approx_idx)
        if start < 0:
            start = q.lower().find(cue_trim)
        if start >= 0:
            tail_orig = q[start + len(cue_trim):].lstrip()
        else:
            # Fallback to lowercase-based split when exact pos not found
            tail_orig = ql.split(best_cue, 1)[1].strip()

        def _extract_paren_span(s: str) -> Tuple[str, str] | Tuple[None, str]:
            if not s or not s.startswith("("):
                return None, s
            depth = 0
            for i, ch in enumerate(s):
                if ch == "(":
                    depth += 1
                elif ch == ")":
                    depth -= 1
                    if depth == 0:
                        # include up to i
                        return s[1:i], s[i + 1 :]
            # Unbalanced: treat as no parens
            return None, s

        # Prefer tokens inside balanced parentheses if present
        in_parens, rest_after = _extract_paren_span(tail_orig)
        tokens_source = None
        if isinstance(in_parens, str):
            tokens_source = in_parens
        else:
            tokens_source = tail_orig

        # If we didn't use parentheses, cut tail at alias assignment to avoid swallowing EQ parts
        aliases = _eq_alias_keys()
        if not in_parens and isinstance(tokens_source, str) and aliases:
            tokens_source = _cut_tail_at_alias(tokens_source, aliases)

        # Split to OR groups first; allow separators: "or", "|", and commas
        raw_groups = [g for g in re.split(r"\s*\bor\b\s*|\s*\|\s*|,", tokens_source, flags=re.I) if g]
        groups: List[List[str]] = []
        for g in raw_groups:
            terms = [t for t in re.split(r"\s*\band\b\s*|&", g, flags=re.I) if t]
            normalized: List[str] = []
            for t in terms:
                token = _norm(t)
                if not token:
                    continue
                if is_email(token) or is_phone(token):
                    continue
                # Skip alias assignments like 'DEPARTMENTS = X'
                if aliases:
                    alias_pat = r"(?:%s)" % "|".join(map(re.escape, aliases))
                    if re.match(rf"(?i)^\s*({alias_pat})\b\s*(?:=|\bin\b|\blike\b|\bbetween\b)", token):
                        continue
                if _looks_like_numeric_clause(token):
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
