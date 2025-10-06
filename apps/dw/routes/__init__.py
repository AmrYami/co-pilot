"""Simplified DW blueprint exposing /dw/answer and /dw/rate."""

from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List, Tuple

from flask import Blueprint, jsonify, request

from apps.dw.db import fetch_rows
from apps.dw.fts_like import build_fts_where
from apps.dw.eq_parser import extract_eq_filters_from_natural_text, strip_eq_from_text
from apps.dw.rate_grammar import parse_rate_comment
from apps.dw.settings_access import DWSettings
from apps.dw.settings_defaults import DEFAULT_EXPLICIT_FILTER_COLUMNS
from apps.dw.fts_utils import DEFAULT_CONTRACT_FTS_COLUMNS

try:  # pragma: no cover - lightweight fallback for tests without settings backend
    from apps.dw.settings_util import get_setting as _get_setting
except Exception:  # pragma: no cover
    def _get_setting(key: str, *, scope=None, namespace=None, default=None):
        return default

bp = Blueprint("dw", __name__)

_GROSS_EXPR = (
    "NVL(CONTRACT_VALUE_NET_OF_VAT,0) + CASE WHEN NVL(VAT,0) BETWEEN 0 AND 1 "
    "THEN NVL(CONTRACT_VALUE_NET_OF_VAT,0) * NVL(VAT,0) ELSE NVL(VAT,0) END"
)


def _settings_dict() -> Dict[str, Any]:
    keys = [
        "DW_FTS_ENGINE",
        "DW_FTS_COLUMNS",
        "DW_EXPLICIT_FILTER_COLUMNS",
        "DW_ENUM_SYNONYMS",
    ]
    cfg: Dict[str, Any] = {}
    for key in keys:
        value = _get_setting(key, scope="namespace", namespace="dw::common", default=None)
        if value is not None:
            cfg[key] = value
    return cfg


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


def _extract_fts_groups(question: str, explicit_cols: List[str]) -> Tuple[List[List[str]], str]:
    if not question:
        return [], "OR"
    without_eq = strip_eq_from_text(question, explicit_cols)
    segment = _after_marker(without_eq)
    lower_segment = segment.lower()
    operator = "OR"
    if " and " in lower_segment:
        operator = "AND"
    elif " or " in lower_segment:
        operator = "OR"
    tokens = _split_tokens(segment)
    groups = [[tok] for tok in tokens if tok]
    return groups, operator


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

    token_groups, token_operator = _extract_fts_groups(question, explicit_cols)
    should_enable_fts = full_text_flag or bool(token_groups)
    fts_sql = ""
    fts_binds: Dict[str, Any] = {}
    fts_error: str | None = None

    engine_raw = settings.get("DW_FTS_ENGINE")
    engine = settings.get_fts_engine()
    if isinstance(engine_raw, str) and engine_raw.strip().lower() not in {"like", "oracle-text"}:
        fts_error = "no_engine -> use LIKE"

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
    debug = {
        "fts": {
            "enabled": bool(fts_sql),
            "tokens": flat_tokens if fts_sql else None,
            "columns": fts_columns if fts_sql else None,
            "binds": list(fts_binds.keys()) if fts_binds else None,
            "error": fts_error,
        },
        "intent": {
            "full_text_search": bool(fts_sql),
            "fts_tokens": flat_tokens,
            "fts_operator": token_operator if fts_sql else None,
            "eq_filters": [
                {"col": col, "val": val} for col, val in eq_pairs
            ],
        },
    }

    meta = {
        "binds": binds,
        "strategy": "fts_like" if fts_sql else ("eq_only" if eq_sqls else "deterministic"),
        "fts": {
            "enabled": bool(fts_sql),
            "mode": "explicit" if fts_sql else None,
            "columns": fts_columns if fts_sql else [],
            "binds": list(fts_binds.keys()) if fts_binds else [],
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
    fts_columns = _fts_columns_from_settings(settings)

    hints = parse_rate_comment(comment)

    fts_sql = ""
    fts_binds: Dict[str, Any] = {}
    fts_error: str | None = None
    engine_raw = settings.get("DW_FTS_ENGINE")
    if isinstance(engine_raw, str) and engine_raw.strip().lower() not in {"like", "oracle-text"}:
        fts_error = "no_engine -> use LIKE"

    tokens = hints.get("fts_tokens") or []
    if tokens and fts_columns:
        fts_sql, fts_binds = build_fts_where([[tok] for tok in tokens], fts_columns, "OR")

    eq_sqls: List[str] = []
    eq_binds: Dict[str, Any] = {}
    for idx, eq in enumerate(hints.get("eq_filters") or []):
        col = str(eq.get("col") or "").upper()
        val = str(eq.get("val") or "")
        sql_piece, binds = _eq_sql(col, val, idx, _apply_eq_synonyms_if_needed(settings, col, val))
        eq_sqls.append(sql_piece)
        eq_binds.update(binds)

    where_sql, binds = _compose_where(fts_sql, fts_binds, eq_sqls, eq_binds)

    group_cols = hints.get("group_by") or []
    sort_by = hints.get("sort_by") or ("CNT" if group_cols else "REQUEST_DATE")
    sort_desc = hints.get("sort_desc")
    if sort_desc is None:
        sort_desc = True

    if group_cols:
        group_sql = "GROUP BY " + ", ".join(group_cols)
        if hints.get("gross"):
            select_sql = f"{group_cols[0]} AS GROUP_KEY, SUM({_GROSS_EXPR}) AS TOTAL_GROSS, COUNT(*) AS CNT"
            order_col = sort_by or "TOTAL_GROSS"
        else:
            select_sql = f"{group_cols[0]} AS GROUP_KEY, COUNT(*) AS CNT"
            order_col = sort_by or "CNT"
        order_sql = f"ORDER BY {order_col} {'DESC' if sort_desc else 'ASC'}"
        parts = [f"SELECT {select_sql}", 'FROM "Contract"']
        if where_sql:
            parts.append(where_sql)
        parts.append(group_sql)
        parts.append(order_sql)
        final_sql = "\n".join(part for part in parts if part)
    else:
        order_col = sort_by or "REQUEST_DATE"
        order_sql = f"ORDER BY {order_col} {'DESC' if sort_desc else 'ASC'}"
        parts = ['SELECT *', 'FROM "Contract"']
        if where_sql:
            parts.append(where_sql)
        parts.append(order_sql)
        final_sql = "\n".join(part for part in parts if part)

    rows = fetch_rows(final_sql, binds)

    debug = {
        "fts": {
            "enabled": bool(fts_sql),
            "tokens": tokens or None,
            "columns": fts_columns if fts_sql else None,
            "binds": fts_binds if fts_binds else None,
            "error": fts_error,
        },
        "intent": {
            "full_text_search": bool(tokens),
            "fts_tokens": tokens or None,
            "fts_columns": fts_columns if fts_sql else [],
            "fts_operator": "OR" if tokens else None,
            "eq_filters": hints.get("eq_filters") or [],
            "group_by": group_cols or None,
            "sort_by": order_col,
            "sort_desc": sort_desc,
        },
        "validation": {
            "ok": True,
            "bind_names": list(binds.keys()),
            "binds": list(binds.keys()),
            "errors": [],
        },
        "rate_hints": {
            "comment_present": bool(comment),
            "where_applied": bool(where_sql),
            "order_by_applied": True,
            "eq_filters": len(hints.get("eq_filters") or []),
        },
    }

    meta = {
        "attempt_no": 2,
        "binds": binds,
    }

    response = {
        "ok": True,
        "inquiry_id": inquiry_id,
        "sql": final_sql,
        "debug": debug,
        "meta": meta,
        "rows": rows,
    }
    return jsonify(response)


__all__ = ["bp", "answer", "rate"]
