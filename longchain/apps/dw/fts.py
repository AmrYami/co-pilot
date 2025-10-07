"""Full-text search helpers for the lightweight DW blueprint."""
from __future__ import annotations

import re
from typing import Dict, List, Optional, Tuple

from .settings import DWSettings, get_fts_columns, get_fts_engine
from .sql_utils import like_expr

Token = str
TokenGroup = List[Token]
FTSInput = List[TokenGroup]

_STOP_WORDS = {
    "the",
    "a",
    "an",
    "of",
    "in",
    "on",
    "at",
    "for",
    "to",
    "by",
    "with",
    "has",
    "have",
    "where",
    "all",
    "list",
    "show",
    "and",
    "or",
}

_EQ_FILTER_RE = re.compile(
    r"[A-Za-z0-9_\"\.]+\s*=\s*(?:'[^']*'|\"[^\"]*\"|[A-Za-z0-9_./-]+)",
    re.IGNORECASE,
)
_AND_SPLIT_RE = re.compile(r"(?i)\band\b")
_OR_SPLIT_RE = re.compile(r"(?i)\bor\b")
_PUNCT_SPLIT_RE = re.compile(r"[,\u060C;؛]+")
_WS_RE = re.compile(r"\s+")

_dw_settings = DWSettings()


def _normalize(text: str) -> str:
    return _WS_RE.sub(" ", (text or "").strip()).lower()


def _strip_eq_sections(text: str) -> str:
    return _EQ_FILTER_RE.sub(" ", text or "")


def _keywordize(chunk: str) -> List[str]:
    words = [w for w in _WS_RE.split(chunk) if w]
    keywords: List[str] = []
    current: List[str] = []
    for word in words:
        lowered = word.lower()
        if lowered in _STOP_WORDS:
            if current:
                keywords.append(" ".join(current))
                current = []
            continue
        current.append(lowered)
    if current:
        keywords.append(" ".join(current))
    return keywords


def _extract_group_tokens(part: str) -> List[str]:
    part = _normalize(part)
    if not part:
        return []
    # Remove punctuation and collapse whitespace again after normalization.
    part = _PUNCT_SPLIT_RE.sub(" ", part)
    part = _normalize(part)

    raw_tokens = [_normalize(tok) for tok in _OR_SPLIT_RE.split(part) if _normalize(tok)]
    tokens: List[str] = []
    for raw in raw_tokens:
        keywords = _keywordize(raw)
        if keywords:
            tokens.extend(keywords)
        else:
            if raw and raw not in _STOP_WORDS:
                tokens.append(raw)
    # Deduplicate while preserving order
    seen = set()
    result = []
    for token in tokens:
        if token and token not in seen:
            seen.add(token)
            result.append(token)
    return result


def parse_tokens(raw_tokens: List[str]) -> FTSInput:
    """Parse caller-provided *raw_tokens* into AND/OR groups."""

    if not raw_tokens:
        return []
    text = " ".join(token for token in raw_tokens if token)
    text = _strip_eq_sections(text)
    text = _PUNCT_SPLIT_RE.sub(" ", text)
    text = _normalize(text)
    if not text:
        return []

    parts = [seg for seg in _AND_SPLIT_RE.split(text) if seg]
    if len(parts) > 1:
        groups: FTSInput = []
        for seg in parts:
            tokens = _extract_group_tokens(seg)
            if tokens:
                groups.append(tokens)
        return groups

    tokens = _extract_group_tokens(text)
    if not tokens:
        return []
    if len(tokens) == 1:
        keywords = _keywordize(tokens[0])
        if len(keywords) > 1:
            return [[tok] for tok in keywords]
    return [[tok] for tok in tokens]


def _resolve_columns(table: str) -> List[str]:
    cols = _dw_settings.fts_columns(table)
    if cols:
        return cols
    return get_fts_columns(table)


def _resolve_engine(default: str = "like") -> str:
    engine = _dw_settings.fts_engine()
    if engine:
        return engine
    return get_fts_engine(default)


def build_fts_where(
    table: str,
    question: str,
    force_operator: Optional[str] = None,
) -> Tuple[str, Dict[str, Any], Dict[str, Any]]:
    """Build a WHERE fragment for FTS using configured columns."""

    engine = _resolve_engine("like")
    columns = _resolve_columns(table or "Contract")

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

    groups = parse_tokens([question])
    if not groups:
        debug["error"] = "no_tokens"
        return "", {}, debug

    lower_question = (question or "").lower()
    if force_operator:
        op = force_operator.strip().upper()
    elif " and " in lower_question:
        op = "AND"
    else:
        op = "OR"
    if op not in {"AND", "OR"}:
        op = "OR"

    binds: Dict[str, Any] = {}
    group_sqls: List[str] = []
    bind_idx = 0
    debug_tokens: List[str] = []

    for group in groups:
        per_token_clauses: List[str] = []
        for token in group:
            token = token.strip()
            if not token:
                continue
            bind_name = f"fts_{bind_idx}"
            bind_idx += 1
            debug_tokens.append(token)
            if engine == "oracle-text":
                binds[bind_name] = token
                col_exprs = [f"CONTAINS({col}, :{bind_name}) > 0" for col in columns]
            else:
                binds[bind_name] = f"%{token}%"
                col_exprs = [like_expr(col, bind_name, oracle=True) for col in columns]
            per_token_clauses.append("( " + " OR ".join(col_exprs) + " )")
        if per_token_clauses:
            if len(per_token_clauses) == 1:
                group_sqls.append(per_token_clauses[0])
            else:
                group_sqls.append("( " + " OR ".join(per_token_clauses) + " )")

    if not group_sqls:
        debug["error"] = "no_tokens"
        return "", {}, debug

    if len(group_sqls) == 1:
        where_sql = group_sqls[0]
    else:
        glue = f" {op} "
        where_sql = glue.join(f"( {clause} )" if not clause.strip().startswith("(") else clause for clause in group_sqls)

    debug["enabled"] = True
    debug["operator"] = op
    debug["binds"] = dict(binds)
    debug["tokens"] = debug_tokens

    return where_sql, binds, debug


__all__ = [
    "Token",
    "TokenGroup",
    "FTSInput",
    "parse_tokens",
    "build_fts_where",
]
