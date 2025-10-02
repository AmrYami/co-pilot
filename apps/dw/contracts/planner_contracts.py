"""Helper utilities for Contract planner equality aliases and FTS."""
from __future__ import annotations

import re
from typing import Dict, List, Optional, Set

from apps.dw.fts import extract_fts_tokens
from apps.dw.fts_utils import build_boolean_fts_where, resolve_fts_columns
from apps.dw.settings import get_fts_columns, get_short_token_allow
from apps.dw.settings_util import get_fts_columns_for

from .columns_map import COLUMN_SYNONYMS, STAKEHOLDER_COLUMNS
from .fts import normalize_terms, parse_fts_terms_from_question


def _sanitize_columns(columns: Optional[List[str]]) -> List[str]:
    seen: Set[str] = set()
    out: List[str] = []
    for col in columns or []:
        if not isinstance(col, str):
            continue
        norm = col.strip().strip('"')
        if not norm:
            continue
        up = norm.upper()
        if up not in seen:
            seen.add(up)
            out.append(up)
    return out


def _settings_getter_from(source):
    def _getter(key: str, default=None):
        if source is None:
            return default
        getter_json = getattr(source, "get_json", None)
        if callable(getter_json):
            try:
                value = getter_json(key, default)
            except TypeError:
                value = getter_json(key)
            if value is not None:
                return value
        if isinstance(source, dict):
            return source.get(key, default)
        getter_plain = getattr(source, "get", None)
        if callable(getter_plain):
            try:
                value = getter_plain(key, default)
            except TypeError:
                value = getter_plain(key)
            if value is not None:
                return value
        return default

    return _getter


def apply_full_text_search(
    db,
    question: str,
    full_text_search: bool,
    base_table: str,
    where_clauses: List[str],
    binds: Dict[str, object],
    debug: Dict[str, object],
    *,
    columns_override: Optional[List[str]] = None,
) -> bool:
    """Append a LIKE-based FTS predicate when possible."""

    if columns_override:
        fts_cols = _sanitize_columns(columns_override)
    else:
        settings_map = db if isinstance(db, dict) else None
        fts_cols = _sanitize_columns(get_fts_columns_for(base_table, config=settings_map))
        if not fts_cols:
            fts_cols = _sanitize_columns(get_fts_columns(db, base_table))
    if not fts_cols:
        getter = _settings_getter_from(db)
        fts_cols = _sanitize_columns(resolve_fts_columns(getter, base_table))
    fts_meta: Dict[str, object] = {}
    debug.setdefault("fts", fts_meta)

    if not fts_cols:
        fts_meta.update({"enabled": False, "error": "no_columns", "columns": []})
        return False

    short_allow = get_short_token_allow(db)
    before_keys = set(binds.keys())
    tokens = extract_fts_tokens(question or "")
    terms = normalize_terms(tokens, short_allow)

    if full_text_search and not terms:
        fallback_terms = parse_fts_terms_from_question(question or "")
        if not fallback_terms:
            fallback_terms = re.findall(r"[A-Za-z0-9][A-Za-z0-9\- ]{2,}", question or "")
        terms = normalize_terms(fallback_terms, short_allow)

    if not terms:
        fts_meta.update({"enabled": False, "error": "no_terms", "columns": fts_cols})
        return False

    existing = len([k for k in binds.keys() if isinstance(k, str) and k.startswith("fts_")])
    prefix = "fts" if existing == 0 else f"fts{existing}"
    raw_sql, binds, join_op = build_boolean_fts_where(
        question_text=question or "",
        terms=terms,
        fts_columns=fts_cols,
        binds=binds,
        bind_prefix=prefix,
    )
    if not raw_sql:
        fts_meta.update({"enabled": False, "error": "build_failed", "columns": fts_cols})
        return False

    where_clauses.append("(" + raw_sql + ")")
    new_bind_keys = [
        k
        for k in binds.keys()
        if isinstance(k, str)
        and k not in before_keys
        and k.startswith(f"{prefix}_")
    ]
    fts_meta.update(
        {
            "enabled": True,
            "columns": fts_cols,
            "tokens": terms,
            "mode": "override" if full_text_search else "implicit",
            "binds": new_bind_keys,
            "join": join_op,
        }
    )
    return True


def apply_equality_aliases(
    question: str,
    where_clauses: List[str],
    binds: Dict[str, object],
    debug: Dict[str, object],
) -> Dict[str, object]:
    """Detect equality aliases like departments = ... or stakeholder has ..."""

    handled_columns: Set[str] = set()
    meta = debug.setdefault("eq_alias", {})
    applied: Dict[str, object] = {"handled_columns": handled_columns, "stakeholder": None}

    text = " ".join((question or "").strip().split())
    if not text:
        return applied

    dept_match = re.search(r"\bDEPARTMENTS?\s*=\s*['\"]?([^'\"\n]+)['\"]?", text, flags=re.IGNORECASE)
    if dept_match:
        value = dept_match.group(1).strip()
        columns = COLUMN_SYNONYMS.get("DEPARTMENTS", ["OWNER_DEPARTMENT"])
        bind_name = "eq_dept_0"
        binds[bind_name] = value
        clause = "(" + " OR ".join(
            [f"UPPER(TRIM({col})) = UPPER(:{bind_name})" for col in columns]
        ) + ")"
        where_clauses.append(clause)
        meta["departments"] = {"value": value, "columns": columns}
        handled_columns.update(columns)

    stk_match = re.search(r"\b(STACKHOLDER|STAKEHOLDER)S?\s+HAS\s+(.+)$", text, flags=re.IGNORECASE)
    if stk_match:
        tail = stk_match.group(2)
        raw_parts = re.split(r"\s*(?:OR|,|/)\s*", tail, flags=re.IGNORECASE)
        terms = [part.strip(" ' \"") for part in raw_parts if part.strip(" ' \"")]
        terms = terms[:10]
        cols = COLUMN_SYNONYMS.get("STAKEHOLDER", STAKEHOLDER_COLUMNS)
        ors: List[str] = []
        for idx, term in enumerate(terms):
            bind = f"eq_stk_{idx}"
            binds[bind] = f"%{term}%"
            ors.append(
                "(" + " OR ".join(
                    [f"UPPER(TRIM({col})) LIKE UPPER(:{bind})" for col in cols]
                ) + ")"
            )
        if ors:
            where_clauses.append("(" + " OR ".join(ors) + ")")
            meta["stakeholder"] = {"values": terms, "columns": cols}
            handled_columns.update(cols)
            applied["stakeholder"] = terms

    return applied


__all__ = ["apply_full_text_search", "apply_equality_aliases"]
