# -*- coding: utf-8 -*-
"""
Text/equality filter extraction for Contract.
- Detect "has ..." or "where has ..." → FTS tokens (supports "or"/"and")
- Detect "where <column> = <value>" equality
- Build SQL fragments for Oracle (UPPER/TRIM LIKE) with safe binds
"""
import re
from typing import Dict, List, Optional, Tuple

from .column_synonyms import normalize_column_name, CONTRACT_STAKEHOLDER_COLS

_HAS_RE = re.compile(r"\b(?:has|where has)\s+(.+)$", re.IGNORECASE)
_EQ_RE  = re.compile(r"\b(?:where|with)\s+([a-zA-Z0-9_ \-]+?)\s*=\s*'?(.+?)'?\s*(?:$|[.;,])", re.IGNORECASE)

def _split_terms(s: str) -> List[str]:
    """
    Split by ' or ' / ' and ' / commas. Preserve multi-word phrases.
    """
    # replace common separators with a uniform comma
    s = re.sub(r"\s+(or|and)\s+", ",", s, flags=re.IGNORECASE)
    s = s.replace("،", ",")
    parts = [p.strip() for p in s.split(",")]
    return [p for p in parts if p]

def extract_has_tokens(question: str) -> Tuple[List[str], Optional[str]]:
    """
    Returns (tokens, narrowed_domain) where narrowed_domain can be:
      - "STAKEHOLDER*" to search only stakeholder columns (1..8)
      - "OWNER_DEPARTMENT" to search only department columns
      - None → search DW_FTS_COLUMNS
    Heuristic narrowing by presence of words "stakeholder"/"department(s)" in question.
    """
    if not question:
        return [], None

    m = _HAS_RE.search(question)
    if not m:
        return [], None

    phrase = m.group(1).strip()
    tokens = _split_terms(phrase)

    narrowed: Optional[str] = None
    if re.search(r"\bstack?holders?\b", question, flags=re.IGNORECASE):
        narrowed = "STAKEHOLDER*"
    elif re.search(r"\bdepartments?\b", question, flags=re.IGNORECASE):
        narrowed = "OWNER_DEPARTMENT"

    return tokens, narrowed

def extract_eq_filters(question: str) -> List[Dict]:
    """
    Extract 'where <column> = <value>' pairs from the question.
    Returns list of dicts: {col, val, ci, trim}
    """
    out: List[Dict] = []
    for col_h, val in _EQ_RE.findall(question or ""):
        norm = normalize_column_name(col_h)
        if not norm:
            # try raw uppercase (user may have written exact DB column)
            raw = col_h.strip().upper().replace(" ", "_").replace("-", "_")
            norm = raw if re.match(r"^[A-Z0-9_]+$", raw) else None
        if not norm:
            continue
        out.append({
            "col": norm,
            "val": val.strip(),
            "ci": True,   # case-insensitive by default
            "trim": True, # trim by default
        })
    return out

def _escape_like(s: str) -> str:
    """Escape % and _ for LIKE patterns."""
    return s.replace("%", r"\%").replace("_", r"\_")

def _word_boundary_regex(term: str) -> str:
    """
    Build a regex for whole-word match for short terms like IT to avoid matching AUDIT.
    Uppercased matching in SQL side.
    """
    t = re.sub(r"\W+", "", term.upper())
    return rf"(^|[^A-Z]){re.escape(t)}([^A-Z]|$)"

def build_fts_where(tokens: List[str], columns: List[str], binds: Dict, bind_prefix="fts") -> Tuple[str, Dict]:
    """
    Build an OR-ed FTS WHERE for multiple columns.
    For short token 'it', use REGEXP_LIKE to enforce boundaries.
    """
    if not tokens or not columns:
        return "", binds

    clauses: List[str] = []
    for idx, term in enumerate(tokens):
        if not term:
            continue

        cleaned = term.strip()
        # special case for very short tokens (like "it") or alphanumeric IDs with digits
        has_digit = any(ch.isdigit() for ch in cleaned)
        is_short = len(cleaned) <= 2 or cleaned.lower() in {"it"} or has_digit
        if is_short:
            # boundary regex (case-insensitive by UPPER on column)
            regex = _word_boundary_regex(term)
            for col in columns:
                clauses.append(f"REGEXP_LIKE(UPPER({col}), :{bind_prefix}_re_{idx})")
            binds[f"{bind_prefix}_re_{idx}"] = regex
        else:
            like = f"%{_escape_like(term)}%"
            escape_fragment = "ESCAPE '\\'"
            for col in columns:
                clauses.append(
                    f"UPPER(TRIM({col})) LIKE UPPER(:{bind_prefix}_{idx}) {escape_fragment}"
                )
            binds[f"{bind_prefix}_{idx}"] = like

    if not clauses:
        return "", binds

    # Group by column for each term? We already appended per-column OR; wrap all in one big OR
    where = "(" + " OR ".join(clauses) + ")"
    return where, binds

def build_eq_where(eq_filters: List[Dict], binds: Dict, bind_prefix="eq") -> Tuple[str, Dict]:
    """
    Build AND-ed equality WHERE. Expand STAKEHOLDER* to 8 columns OR-ed.
    """
    if not eq_filters:
        return "", binds

    and_parts: List[str] = []
    for i, f in enumerate(eq_filters):
        col = f["col"]
        val = f["val"]
        ci  = bool(f.get("ci", True))
        tr  = bool(f.get("trim", True))
        bind_key = f"{bind_prefix}_{i}"

        if col == "STAKEHOLDER*":
            ors: List[str] = []
            for c in CONTRACT_STAKEHOLDER_COLS:
                if ci and tr:
                    ors.append(f"UPPER(TRIM({c})) = UPPER(TRIM(:{bind_key}))")
                elif ci:
                    ors.append(f"UPPER({c}) = UPPER(:{bind_key})")
                elif tr:
                    ors.append(f"TRIM({c}) = TRIM(:{bind_key})")
                else:
                    ors.append(f"{c} = :{bind_key}")
            and_parts.append("(" + " OR ".join(ors) + ")")
            binds[bind_key] = val
        else:
            target = col
            if ci and tr:
                and_parts.append(f"UPPER(TRIM({target})) = UPPER(TRIM(:{bind_key}))")
            elif ci:
                and_parts.append(f"UPPER({target}) = UPPER(:{bind_key})")
            elif tr:
                and_parts.append(f"TRIM({target}) = TRIM(:{bind_key})")
            else:
                and_parts.append(f"{target} = :{bind_key}")
            binds[bind_key] = val

    return " AND ".join(and_parts), binds
