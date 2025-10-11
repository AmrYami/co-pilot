"""Simplified DW blueprint exposing /dw/answer and /dw/rate."""

from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List, Tuple

from flask import Blueprint, jsonify, request

from apps.dw.db import fetch_rows
from apps.dw.fts_like import build_fts_where
from apps.dw.eq_parser import extract_eq_filters_from_natural_text, strip_eq_from_text
from apps.dw.settings_access import DWSettings
from apps.dw.settings_defaults import DEFAULT_EXPLICIT_FILTER_COLUMNS
from apps.dw.fts_utils import DEFAULT_CONTRACT_FTS_COLUMNS
from apps.dw.sqlbuilder import (
    build_sql_from_intent,
    direction_from_words,
    parse_rate_comment as parse_rate_comment_v2,
)

try:  # pragma: no cover - lightweight fallback for tests without settings backend
    from apps.dw.settings_util import get_setting as _get_setting
except Exception:  # pragma: no cover
    def _get_setting(key: str, *, scope=None, namespace=None, default=None):
        return default

bp = Blueprint("dw", __name__)


def _settings_dict() -> Dict[str, Any]:
    keys = [
        "DW_FTS_ENGINE",
        "DW_FTS_COLUMNS",
        "DW_FTS_MIN_TOKEN_LEN",
        "DW_EXPLICIT_FILTER_COLUMNS",
        "DW_ENUM_SYNONYMS",
        "DW_CONTRACT_TABLE",
        "DW_EQ_ALIAS_COLUMNS",
        "DW_DATE_COLUMN",
    ]
    namespace_cfg: Dict[str, Any] = {}
    global_cfg: Dict[str, Any] = {}
    for key in keys:
        value = _get_setting(key, scope="namespace", namespace="dw::common", default=None)
        if value is not None:
            namespace_cfg[key] = value
        global_value = _get_setting(key, scope="namespace", namespace="global", default=None)
        if global_value is not None:
            global_cfg[key] = global_value
    return {"__namespace__": namespace_cfg, "__global__": global_cfg}


def _load_dw_settings() -> DWSettings:
    return DWSettings(_settings_dict())


def _dedupe_columns(columns: Iterable[str]) -> List[str]:
    seen: set[str] = set()
    result: List[str] = []
    for raw in columns:
        if not isinstance(raw, str):
            continue
        text = raw.strip()
        if not text:
            continue
        if text.startswith('"') and text.endswith('"'):
            key = text
        else:
            key = text.upper()
        if key in seen:
            continue
        seen.add(key)
        result.append(key)
    return result


def _fts_columns_from_settings(settings: DWSettings) -> List[str]:
    contract_cols, wildcard_cols = settings.get_fts_columns()
    candidates = contract_cols or wildcard_cols
    if not candidates:
        candidates = DEFAULT_CONTRACT_FTS_COLUMNS
    cleaned = [str(col) for col in candidates if isinstance(col, str) and str(col).strip()]
    return _dedupe_columns(cleaned)


def _explicit_columns(settings: DWSettings) -> List[str]:
    explicit = settings.get_explicit_eq_columns()
    if not explicit:
        explicit = DEFAULT_EXPLICIT_FILTER_COLUMNS
    normalized: List[str] = []
    seen: set[str] = set()
    for raw in explicit:
        if not isinstance(raw, str):
            continue
        text = raw.strip()
        if not text:
            continue
        if text.startswith('"') and text.endswith('"'):
            key = text
        else:
            key = re.sub(r"\s+", "_", text.upper())
        if key in seen:
            continue
        seen.add(key)
        normalized.append(key)
    return normalized


def _apply_eq_synonyms_if_needed(
    settings: DWSettings, col: str, val: str
) -> Tuple[str, List[str], List[str], List[str]]:
    if col.upper() != "REQUEST_TYPE":
        return "none", [], [], []

    mapping = settings.get_request_type_synonyms()
    if not mapping:
        return "none", [], [], []

    value_upper = (val or "").strip().upper()
    for key, spec in mapping.items():
        equals_raw = [x for x in (spec.get("equals") or []) if isinstance(x, str) and x.strip()]
        prefix_raw = [x for x in (spec.get("prefix") or []) if isinstance(x, str) and x.strip()]
        contains_raw = [x for x in (spec.get("contains") or []) if isinstance(x, str) and x.strip()]
        equals_upper = [x.strip().upper() for x in equals_raw]
        if value_upper == str(key).strip().upper() or value_upper in equals_upper:
            return (
                "request_type",
                equals_upper or [value_upper],
                [x.strip().upper() for x in prefix_raw],
                [x.strip().upper() for x in contains_raw],
            )
    return "none", [], [], []


def _eq_sql(
    col: str,
    val: str,
    idx: int,
    synonyms: Tuple[str, List[str], List[str], List[str]],
) -> Tuple[str, Dict[str, Any]]:
    mode, equals, prefix, contains = synonyms
    binds: Dict[str, Any] = {}
    column_expr = col.strip().upper()
    if not column_expr.startswith('"'):
        column_expr = column_expr.replace(" ", "_")
    if mode == "request_type":
        predicates: List[str] = []
        if equals:
            in_names: List[str] = []
            for j, v in enumerate(equals):
                bind = f"eq_{idx}_eq_{j}"
                binds[bind] = v
                in_names.append(f":{bind}")
            predicates.append(f"UPPER(TRIM({column_expr})) IN ({', '.join(in_names)})")
        for j, v in enumerate(prefix):
            bind = f"eq_{idx}_pr_{j}"
            binds[bind] = f"{v}%"
            predicates.append(f"UPPER(TRIM({column_expr})) LIKE UPPER(:{bind})")
        for j, v in enumerate(contains):
            bind = f"eq_{idx}_ct_{j}"
            binds[bind] = f"%{v}%"
            predicates.append(f"UPPER(TRIM({column_expr})) LIKE UPPER(:{bind})")
        if not predicates:
            bind = f"eq_{idx}"
            binds[bind] = val
            predicates.append(f"UPPER(TRIM({column_expr})) = UPPER(TRIM(:{bind}))")
        return "(" + " OR ".join(predicates) + ")", binds

    bind = f"eq_{idx}"
    binds[bind] = val
    return f"UPPER(TRIM({column_expr})) = UPPER(TRIM(:{bind}))", binds


def _compose_where(
    fts_sql: str,
    fts_binds: Dict[str, Any],
    eq_sqls: List[str],
    eq_binds: Dict[str, Any],
) -> Tuple[str, Dict[str, Any]]:
    parts: List[str] = []
    if fts_sql:
        parts.append(fts_sql)
    if eq_sqls:
        parts.append("(" + " AND ".join(eq_sqls) + ")")
    if not parts:
        return "", {}
    combined: Dict[str, Any] = {}
    combined.update(fts_binds or {})
    combined.update(eq_binds or {})
    return "WHERE " + " AND ".join(parts), combined


def _after_marker(text: str) -> str:
    lowered = text.lower()
    markers = [" has ", " have ", " with ", " containing ", " contains ", " include ", " includes "]
    for marker in markers:
        idx = lowered.find(marker)
        if idx != -1:
            return text[idx + len(marker):]
    return text


def _split_tokens(segment: str) -> List[str]:
    tokens: List[str] = []
    buffer: List[str] = []
    lowered = segment.lower()
    i = 0
    while i < len(segment):
        if lowered.startswith(" and ", i):
            token = "".join(buffer).strip(" \t,.;'\"")
            if token:
                tokens.append(token)
            buffer = []
            i += 5
            continue
        if lowered.startswith(" or ", i):
            token = "".join(buffer).strip(" \t,.;'\"")
            if token:
                tokens.append(token)
            buffer = []
            i += 4
            continue
        buffer.append(segment[i])
        i += 1
    tail = "".join(buffer).strip(" \t,.;'\"")
    if tail:
        tokens.append(tail)
    return tokens


def _extract_fts_groups(question: str, explicit_cols: List[str]) -> Tuple[List[List[str]], str, str]:
    if not question:
        return [], "OR", "OR default"
    without_eq = strip_eq_from_text(question, explicit_cols)
    segment = _after_marker(without_eq)
    lower_segment = segment.lower()
    operator = "OR"
    reason = "OR default"
    if " and " in lower_segment:
        operator = "AND"
        reason = "AND because keyword 'and' was detected"
    elif " or " in lower_segment:
        operator = "OR"
        reason = "OR because keyword 'or' was detected"
    tokens = _split_tokens(segment)
    groups = [[tok] for tok in tokens if tok]
    return groups, operator, reason


def _flatten(groups: List[List[str]]) -> List[str]:
    tokens: List[str] = []
    for group in groups:
        tokens.extend(group)
    return tokens


@bp.route("/dw/answer", methods=["POST"])
def answer() -> Any:
    payload = request.get_json(force=True, silent=True) or {}
    question = (payload.get("question") or "").strip()
    full_text_flag = bool(payload.get("full_text_search"))

    settings = _load_dw_settings()
    explicit_cols = _explicit_columns(settings)
    fts_columns = _fts_columns_from_settings(settings)

    raw_eq_pairs = extract_eq_filters_from_natural_text(question, explicit_cols)
    eq_pairs = [
        (str(col).strip().upper().replace(" ", "_"), val)
        for col, val in raw_eq_pairs
        if col
    ]

    eq_sqls: List[str] = []
    eq_binds: Dict[str, Any] = {}
    for idx, (col, val) in enumerate(eq_pairs):
        sql_piece, binds = _eq_sql(col, val, idx, _apply_eq_synonyms_if_needed(settings, col, val))
        eq_sqls.append(sql_piece)
        eq_binds.update(binds)

    token_groups, token_operator, token_reason = _extract_fts_groups(question, explicit_cols)
    should_enable_fts = full_text_flag or bool(token_groups)
    fts_sql = ""
    fts_binds: Dict[str, Any] = {}

    engine = settings.get_fts_engine()

    if should_enable_fts and fts_columns:
        groups = token_groups
        if not groups and question:
            groups = [[question]]
        fts_sql, fts_binds = build_fts_where(groups, fts_columns, token_operator)

    where_sql, binds = _compose_where(fts_sql, fts_binds, eq_sqls, eq_binds)
    order_sql = "ORDER BY REQUEST_DATE DESC"

    parts = ['SELECT * FROM "Contract"']
    if where_sql:
        parts.append(where_sql)
    parts.append(order_sql)
    final_sql = "\n".join(part for part in parts if part)

    rows = fetch_rows(final_sql, binds)

    flat_tokens = _flatten(token_groups) if token_groups else []
    fts_reason = token_reason if (fts_sql or flat_tokens) else None
    eq_applied = [{"col": col, "val": val} for col, val in eq_pairs]
    explain_parts: List[str] = []
    if fts_sql:
        cols_list = ", ".join(str(col) for col in fts_columns) or "(no columns configured)"
        explain_parts.append(
            f"FTS tokens joined with {token_operator} ({token_reason}). Columns: {cols_list}."
        )
    if eq_applied:
        cols = ", ".join(item["col"] for item in eq_applied)
        explain_parts.append(f"Equality filters applied on {cols}.")
    debug = {
        "fts": {
            "enabled": bool(fts_sql),
            "tokens": flat_tokens if fts_sql else None,
            "columns": fts_columns if fts_sql else None,
            "binds": list(fts_binds.keys()) if fts_binds else None,
            "engine": engine,
            "reason": fts_reason,
        },
        "intent": {
            "full_text_search": bool(fts_sql),
            "fts_tokens": flat_tokens,
            "fts_operator": token_operator if fts_sql else None,
            "eq_filters": eq_applied,
        },
        "explain": explain_parts,
    }

    meta = {
        "binds": binds,
        "strategy": "fts_like" if fts_sql else ("eq_only" if eq_sqls else "deterministic"),
        "fts": {
            "enabled": bool(fts_sql),
            "mode": "explicit" if fts_sql else None,
            "columns": fts_columns if fts_sql else [],
            "binds": list(fts_binds.keys()) if fts_binds else [],
            "engine": engine,
            "operator": token_operator if fts_sql else None,
        },
    }

    response = {
        "ok": True,
        "sql": final_sql,
        "meta": meta,
        "debug": debug,
        "rows": rows,
    }
    return jsonify(response)


@bp.route("/dw/rate", methods=["POST"])
def rate() -> Any:
    payload = request.get_json(force=True, silent=True) or {}
    inquiry_id = payload.get("inquiry_id")
    comment = (payload.get("comment") or "").strip()

    settings = _load_dw_settings()
    raw_settings: Dict[str, Any] = dict(settings.global_ns)
    raw_settings.update(settings.ns)

    hints = parse_rate_comment_v2(comment)
    raw_tokens = hints.get("fts_tokens") or []
    operator = (hints.get("fts_operator") or "OR").upper()
    group_col = hints.get("group_by")
    group_cols = [group_col] if group_col else []
    gross_flag = hints.get("gross")
    sort_by_hint = hints.get("sort_by")
    sort_desc_hint = hints.get("sort_desc")
    top_n = hints.get("top_n")
    direction_hint = hints.get("direction_hint")

    contract_table = raw_settings.get("DW_CONTRACT_TABLE") or "Contract"

    fts_cfg = settings.resolve_fts_config(
        tokens=raw_tokens,
        table_name=contract_table,
        namespace="dw::common",
    )
    tokens = fts_cfg.get("tokens") or []
    fts_columns = _dedupe_columns(fts_cfg.get("columns") or [])
    fts_engine = fts_cfg.get("engine") or settings.get_fts_engine()
    if fts_cfg.get("error") == "no_engine":
        fts_engine = "like"

    builder_fts: Dict[str, List[str]] = {}
    raw_fts = raw_settings.get("DW_FTS_COLUMNS")
    if isinstance(raw_fts, dict):
        for key, value in raw_fts.items():
            if isinstance(value, list):
                builder_fts[str(key)] = list(value)
    if fts_columns:
        builder_fts.setdefault(contract_table, fts_columns)
        builder_fts.setdefault(contract_table.upper(), fts_columns)

    explicit_cols = settings.get_explicit_eq_columns() or DEFAULT_EXPLICIT_FILTER_COLUMNS
    if not explicit_cols:
        explicit_cols = DEFAULT_EXPLICIT_FILTER_COLUMNS

    builder_settings = {
        "DW_FTS_COLUMNS": builder_fts,
        "DW_EXPLICIT_FILTER_COLUMNS": explicit_cols,
        "DW_FTS_ENGINE": fts_engine,
        "DW_CONTRACT_TABLE": contract_table,
    }
    alias_map = raw_settings.get("DW_EQ_ALIAS_COLUMNS")
    if alias_map:
        builder_settings["DW_EQ_ALIAS_COLUMNS"] = alias_map
    date_column = raw_settings.get("DW_DATE_COLUMN")
    if date_column:
        builder_settings["DW_DATE_COLUMN"] = date_column

    intent = {
        "date_column": "OVERLAP",
        "fts_tokens": tokens,
        "fts_operator": operator,
        "full_text_search": bool(tokens),
        "eq_filters": hints.get("eq_filters") or [],
        "boolean_groups": hints.get("boolean_groups") or [],
        "group_by": group_col,
        "gross": gross_flag,
        "sort_by": sort_by_hint,
        "sort_desc": sort_desc_hint,
        "top_n": top_n,
        "direction_hint": direction_hint,
        "wants_all_columns": True,
    }

    final_sql, binds, builder_dbg = build_sql_from_intent(intent, builder_settings, table=contract_table)
    rows = fetch_rows(final_sql, binds)

    sort_desc_effective = sort_desc_hint
    if direction_hint is not None and sort_desc_effective is None:
        sort_desc_effective, _ = direction_from_words([direction_hint])
    if sort_desc_effective is None:
        sort_desc_effective = True

    if group_cols:
        order_col = sort_by_hint or ("TOTAL_GROSS" if gross_flag else "CNT")
    else:
        order_col = sort_by_hint or "REQUEST_DATE"

    fts_bind_names = [name for name in binds if name.startswith("fts_")]

    intent_debug = dict(intent)
    intent_debug["sort_by_effective"] = order_col
    intent_debug["sort_desc_effective"] = sort_desc_effective

    debug = {
        "fts": {
            "enabled": bool(tokens),
            "tokens": tokens or None,
            "columns": fts_columns if tokens else None,
            "binds": fts_bind_names or None,
            "engine": builder_settings.get("DW_FTS_ENGINE"),
            "operator": operator if tokens else None,
            "error": fts_cfg.get("error"),
            "min_token_len": fts_cfg.get("min_token_len"),
        },
        "intent": intent_debug,
        "validation": {
            "ok": True,
            "bind_names": list(binds.keys()),
            "binds": list(binds.keys()),
            "errors": [],
        },
        "rate_hints": {
            "comment_present": bool(comment),
            "where_applied": bool(tokens or intent["eq_filters"]),
            "order_by_applied": True,
            "eq_filters": len(intent["eq_filters"]),
        },
        "builder_notes": builder_dbg.get("notes"),
    }

    meta = {
        "attempt_no": 2,
        "binds": binds,
        "fts": {
            "enabled": bool(tokens),
            "engine": builder_settings.get("DW_FTS_ENGINE"),
            "operator": operator if tokens else None,
            "columns": fts_columns if tokens else [],
            "error": fts_cfg.get("error"),
            "min_token_len": fts_cfg.get("min_token_len"),
        },
        "clarifier_intent": intent_debug,
    }

    response = {
        "ok": True,
        "inquiry_id": inquiry_id,
        "sql": final_sql,
        "debug": debug,
        "meta": meta,
        "rows": rows,
        "retry": True,
    }
    return jsonify(response)


__all__ = ["bp", "answer", "rate"]
