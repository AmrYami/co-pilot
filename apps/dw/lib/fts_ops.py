# -*- coding: utf-8 -*-
"""
FTS builder (LIKE-based) with OR/AND token groups.
Respects settings:
  - DW_FTS_COLUMNS (Contract/*)
  - DW_FTS_ENGINE  (currently supports "like"; other values fallback to "like")
"""
from typing import Dict, List, Tuple
import re


def _normalize_tokens(raw: str) -> str:
    # minimal normalization: trim + collapse spaces
    return " ".join((raw or "").strip().split())


# Hygiene: exclude tokens that are clearly EQ-like (emails/phones or explicit
# left-hand sides for known EQ columns) from FTS handling.
EMAIL_RE = re.compile(r"(?i)\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b")
PHONE_RE = re.compile(r"\b(?:\+?\d[\d\s\-]{6,})\b")
EQ_LHS = {"representative_email", "representative_phone", "entity", "entity_no"}


def _scrub_eq_like_tokens(groups: List[List[str]]) -> List[List[str]]:
    cleaned: List[List[str]] = []
    for grp in groups or []:
        out: List[str] = []
        for t in grp or []:
            s = (t or "").strip()
            if not s:
                continue
            if EMAIL_RE.fullmatch(s) or PHONE_RE.fullmatch(s):
                continue
            if "=" in s:
                left = s.split("=", 1)[0].strip().lower()
                if left in EQ_LHS:
                    continue
            out.append(s)
        if out:
            cleaned.append(out)
    return cleaned


def detect_fts_groups(question: str) -> Tuple[List[List[str]], str]:
    """
    Parse tokens from natural phrase.
    - "has it or home care"  -> [["it"], ["home care"]], operator="OR"
    - "has it and home care" -> [["it"], ["home care"]], operator="AND"
    - fallback: one group with one token derived from question keywords after 'has'
    This function is intentionally simple—rate endpoint can override via hints.
    """
    q = (question or "").lower()
    # naive cue words
    # try to extract after "has", "contain", "include"
    cue_idx = -1
    for cue in [" has ", " contain ", " contains ", " include ", " includes "]:
        if cue in q:
            cue_idx = q.index(cue) + len(cue)
            break
    payload = q[cue_idx:].strip() if cue_idx >= 0 else q
    # operator decision
    op = "OR"
    if " and " in payload and " or " not in payload:
        op = "AND"
        parts = [p.strip() for p in payload.split(" and ") if p.strip()]
    elif " or " in payload:
        op = "OR"
        parts = [p.strip() for p in payload.split(" or ") if p.strip()]
    else:
        parts = [payload] if payload else []

    groups: List[List[str]] = []
    for tok in parts:
        if tok:
            groups.append([_normalize_tokens(tok)])
    # filter empties
    groups = [g for g in groups if any(t for t in g)]
    if not groups:
        return [], op
    return groups, op


def resolve_fts_columns(settings: Dict) -> List[str]:
    """
    Pull FTS columns from settings:
      DW_FTS_COLUMNS.value["Contract"] or DW_FTS_COLUMNS.value["*"]
    """
    cfg = (settings or {}).get("DW_FTS_COLUMNS", {}) or {}
    val = cfg.get("value") if isinstance(cfg, dict) else cfg
    if not isinstance(val, dict):
        return []
    cols = val.get("Contract") or val.get("*") or []
    # Normalize to DB-safe uppercase without quoting here; quoting is added in SQL builder
    return [c.strip().upper() for c in cols if c and isinstance(c, str)]


def build_fts_where_like(
    columns: List[str],
    groups: List[List[str]],
    operator: str,
    bind_prefix: str = "fts",
) -> Tuple[str, Dict[str, str], Dict]:
    """
    Build WHERE using LIKE on given columns.
    groups:
      - outer groups combined by `operator` (OR/AND)
      - inside each group we OR the columns for each token (classic FTS any-column match)
    Returns:
      sql_fragment, binds, debug
    """
    binds: Dict[str, str] = {}
    if not columns or not groups:
        return "", binds, {"enabled": False, "error": "no_columns"}
    # scrub PII/EQ-like tokens from FTS
    groups = _scrub_eq_like_tokens(groups)

    # Each token gets a bind like :fts_0, :fts_1, ...
    bind_list = []
    bind_idx = 0
    group_sqls = []

    for group in groups:
        # group has tokens [tokA, tokB, ...] – here we treat group as a single token OR group
        # If you want multi-token group AND inside group, extend here—current design ORs inside group over columns.
        for tok in group:
            pname = f"{bind_prefix}_{bind_idx}"
            binds[pname] = f"%{tok}%"
            bind_list.append(pname)
            col_like_parts = [
                f"UPPER(NVL({col},'')) LIKE UPPER(:{pname})" for col in columns
            ]
            group_sqls.append("(" + " OR ".join(col_like_parts) + ")")
            bind_idx += 1

    if not group_sqls:
        return "", {}, {"enabled": False, "error": "no_tokens"}

    joiner = f" {operator} " if operator in ("OR", "AND") else " OR "
    where_sql = "(" + joiner.join(group_sqls) + ")"
    debug = {
        "enabled": True,
        "tokens": [binds[p] for p in bind_list],
        "columns": columns,
        "binds": {k: binds[k] for k in bind_list},
    }
    return where_sql, binds, debug


def build_fts_where(settings: Dict, groups: List[List[str]], operator: str) -> Tuple[str, Dict[str, str], Dict]:
    """
    Entrypoint based on DW_FTS_ENGINE. Currently supports "like"; any other value falls back to "like".
    """
    engine = ((settings or {}).get("DW_FTS_ENGINE") or {}).get("value") or "like"
    columns = resolve_fts_columns(settings)
    if engine.lower() != "like":
        # fallback to like to avoid "no_engine"
        sql, binds, dbg = build_fts_where_like(columns, groups, operator)
        dbg["engine_fallback"] = "like"
        return sql, binds, dbg
    return build_fts_where_like(columns, groups, operator)
