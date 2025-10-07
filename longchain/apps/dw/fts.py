# longchain/apps/dw/fts.py
# -*- coding: utf-8 -*-
"""
Full-text search (FTS) planner for DW.
Builds safe, engine-aware WHERE clauses from natural-language tokens.

Engine:
- like          -> uses UPPER(NVL(col,'')) LIKE UPPER(:bind)
- oracle-text   -> uses CONTAINS(col, :bind) > 0   (requires Oracle Text)

AND / OR:
- If the question contains explicit 'and' between phrases, groups are ANDed.
- If it contains 'or' (or no explicit connector), groups are ORed by default.

Stop-words:
- Very small set to ignore irrelevant tokens.
"""

from __future__ import annotations
from typing import List, Tuple, Dict, Any, Optional
import re
import logging

from . import settings as dw_settings

log = logging.getLogger(__name__)

_STOP_WORDS = {
    "the", "a", "an", "of", "in", "on", "at", "for", "to", "by", "with",
    "has", "have", "where", "all", "list", "show", "and", "or",
}

_TOKEN_SPLIT_RE = re.compile(r"[,\u060C;؛]+|\\band\\b|\\bor\\b", re.IGNORECASE)
_AND_RE = re.compile(r"\\band\\b", re.IGNORECASE)
_OR_RE  = re.compile(r"\\bor\\b", re.IGNORECASE)
_WS_RE  = re.compile(r"\\s+")

def _normalize_tok(s: str) -> str:
    return _WS_RE.sub(" ", (s or "").strip().lower())

def _is_meaningful(tok: str) -> bool:
    if not tok:
        return False
    if tok in _STOP_WORDS:
        return False
    # token should have at least one alnum
    return any(ch.isalnum() for ch in tok)

def tokenize_fts_query(question: str) -> Tuple[List[List[str]], str]:
    """
    Split the question into token groups and decide the top-level operator.
    Returns (groups, operator) where operator is "AND" or "OR".

    Examples:
      "has it or home care"  -> groups: [["it"], ["home care"]], op="OR"
      "has it and home care" -> groups: [["it"], ["home care"]], op="AND"
      "it"                   -> groups: [["it"]], op="OR"
    """
    q = (question or "").strip()
    if not q:
        return [], "OR"

    # Decide operator: explicit 'and' takes precedence, otherwise 'or', else OR
    op = "OR"
    has_and = bool(_AND_RE.search(q))
    has_or  = bool(_OR_RE.search(q))
    if has_and and not has_or:
        op = "AND"
    elif has_or and not has_and:
        op = "OR"
    elif has_and and has_or:
        # Mixed case: prefer AND if 'and' appears between non-empty chunks
        op = "AND"

    # Split by 'and'/'or'/punctuation, but then reconstruct groups respectfully
    raw_chunks = [c for c in _TOKEN_SPLIT_RE.split(q) if c is not None]
    # Remove empty and normalize
    norm_chunks = [_normalize_tok(c) for c in raw_chunks]
    norm_chunks = [c for c in norm_chunks if _is_meaningful(c)]

    # If nothing left, return empty
    if not norm_chunks:
        return [], op

    # Simple grouping: each chunk is its own group
    groups = [[c] for c in norm_chunks]
    return groups, op

def _escape_like(token: str) -> str:
    # Escape % and _ for LIKE. You can also add ESCAPE clause if needed.
    return token.replace("%", "\\%").replace("_", "\\_")

def _like_clause(col: str, bind: str) -> str:
    return f"UPPER(NVL({col},'')) LIKE UPPER(:{bind})"

def _contains_clause(col: str, bind: str) -> str:
    return f"CONTAINS({col}, :{bind}) > 0"

def build_fts_where(
    table: str,
    question: str,
    force_operator: Optional[str] = None,
) -> Tuple[str, Dict[str, Any], Dict[str, Any]]:
    """
    Build FTS WHERE SQL fragment + binds + debug.
    Returns (where_sql, binds, debug)
    """
    # 1) Read settings
    engine = dw_settings.get_fts_engine("like")
    columns = dw_settings.get_fts_columns(table or "Contract")

    debug: Dict[str, Any] = {
        "enabled": False,
        "mode": "explicit",
        "operator": None,
        "columns": columns or [],
        "tokens": [],
        "binds": {},
        "error": None,
    }

    if not columns:
        debug["error"] = "no_columns"
        return "", {}, debug

    # 2) Tokenize
    groups, default_op = tokenize_fts_query(question)
    if not groups:
        debug["error"] = "no_tokens"
        return "", {}, debug

    op = (force_operator or default_op or "OR").upper()
    if op not in {"AND", "OR"}:
        op = "OR"

    # 3) Build SQL depending on engine
    binds: Dict[str, Any] = {}
    clauses: List[str] = []
    bind_idx = 0

    try:
        for g in groups:
            # g is a list of tokens; combine inner columns by OR for each token
            # (token matches if it appears in any column)
            g_clauses: List[str] = []
            for tok in g:
                bind_name = f"fts_{bind_idx}"
                bind_idx += 1
                debug["tokens"].append(tok)
                if engine == "like":
                    val = f"%{_escape_like(tok)}%"
                    binds[bind_name] = val
                    col_ors = [ _like_clause(c, bind_name) for c in columns ]
                elif engine == "oracle-text":
                    # For Oracle Text we can use the token as-is (or wrap with double quotes)
                    binds[bind_name] = tok
                    col_ors = [ _contains_clause(c, bind_name) for c in columns ]
                else:
                    # Unknown engine -> fallback to like safely
                    val = f"%{_escape_like(tok)}%"
                    binds[bind_name] = val
                    col_ors = [ _like_clause(c, bind_name) for c in columns ]
                g_clauses.append("( " + " OR ".join(col_ors) + " )")

            # Token group itself is an AND of its tokens (e.g. ["home", "care"] inside one chunk)
            if len(g_clauses) == 1:
                clauses.append(g_clauses[0])
            else:
                clauses.append("( " + " AND ".join(g_clauses) + " )")

        # Top-level combine groups by op
        if len(clauses) == 1:
            where_sql = clauses[0]
        else:
            glue = f" {op} "
            where_sql = "( " + glue.join(clauses) + " )"

        debug["enabled"] = True
        debug["operator"] = op
        debug["binds"] = {k: v for k, v in binds.items()}
        return where_sql, binds, debug

    except Exception as ex:
        log.exception("Failed to build FTS WHERE: %s", ex)
        debug["error"] = "exception"
        return "", {}, debug
