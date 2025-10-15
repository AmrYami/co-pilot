"""Simplified DW blueprint exposing /dw/answer and /dw/rate."""

from __future__ import annotations

import json
import logging
import re
import uuid
from typing import Any, Dict, Iterable, List, Tuple

from flask import Blueprint, jsonify, request

from apps.dw.db import fetch_rows
from apps.dw.eq_parser import extract_eq_filters_from_natural_text, strip_eq_from_text
from apps.dw.search import resolve_engine
from apps.dw.settings_access import DWSettings
from apps.dw.settings_defaults import DEFAULT_EXPLICIT_FILTER_COLUMNS
from apps.dw.fts_utils import DEFAULT_CONTRACT_FTS_COLUMNS
from apps.dw.sql import QueryBuilder
from apps.dw.sqlbuilder import (
    direction_from_words,
    parse_rate_comment as parse_rate_comment_v2,
)
from apps.dw.logs import scrub_binds
from apps.dw.utils import env_flag

try:  # pragma: no cover - lightweight fallback for tests without settings backend
    from apps.dw.settings_util import get_setting as _get_setting
except Exception:  # pragma: no cover
    def _get_setting(key: str, *, scope=None, namespace=None, default=None):
        return default

bp = Blueprint("dw", __name__)

logger = logging.getLogger("dw.routes")


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


def _dedupe_upper(values: Iterable[Any]) -> List[str]:
    seen: set[str] = set()
    ordered: List[str] = []
    for raw in values:
        if not isinstance(raw, str):
            continue
        text = raw.strip()
        if not text:
            continue
        upper = text.upper()
        if upper in seen:
            continue
        seen.add(upper)
        ordered.append(upper)
    return ordered


def _collect_request_type_synonyms(
    values: Iterable[str],
    mapping: Dict[str, Dict[str, Iterable[Any]]],
) -> Tuple[List[str], List[str], List[str]]:
    equals: List[str] = []
    prefixes: List[str] = []
    contains: List[str] = []
    for raw in values:
        if not isinstance(raw, str):
            continue
        text = raw.strip()
        if not text:
            continue
        upper = text.upper()
        equals.append(upper)
        for key, spec in mapping.items():
            if not isinstance(spec, dict):
                continue
            key_upper = str(key).strip().upper()
            eq_list = _dedupe_upper(spec.get("equals", []))
            if upper == key_upper or upper in eq_list:
                equals.extend(eq_list or [key_upper])
                prefixes.extend(_dedupe_upper(spec.get("prefix", [])))
                contains.extend(_dedupe_upper(spec.get("contains", [])))
                break
    return _dedupe_upper(equals), _dedupe_upper(prefixes), _dedupe_upper(contains)


def _inject_request_type_synonyms(
    eq_filters: List[Dict[str, Any]], settings: DWSettings
) -> List[Dict[str, Any]]:
    if not eq_filters:
        return []
    mapping = settings.get_request_type_synonyms()
    if not mapping:
        return [dict(entry) for entry in eq_filters if isinstance(entry, dict)]

    enriched: List[Dict[str, Any]] = []
    for entry in eq_filters:
        if not isinstance(entry, dict):
            continue
        updated = dict(entry)
        col = str(updated.get("col") or updated.get("column") or "").strip()
        if col.upper() == "REQUEST_TYPE":
            values: List[str] = []
            if isinstance(updated.get("values"), (list, tuple)):
                values.extend(str(v) for v in updated.get("values") if v is not None)
            fallback = updated.get("val")
            if fallback is not None:
                values.append(str(fallback))
            eq_vals, pref_vals, contains_vals = _collect_request_type_synonyms(values, mapping)
            if eq_vals or pref_vals or contains_vals:
                updated["synonyms"] = {
                    "equals": eq_vals,
                    "prefix": pref_vals,
                    "contains": contains_vals,
                }
                updated["ci"] = True
                updated["trim"] = True
        enriched.append(updated)
    return enriched


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
    eq_filters: List[Dict[str, Any]] = []
    eq_applied: List[Dict[str, Any]] = []
    for col, val in eq_pairs:
        mode, equals, prefix, contains = _apply_eq_synonyms_if_needed(settings, col, val)
        eq_applied.append({"col": col, "val": val})
        if mode != "none":
            eq_filters.append(
                {
                    "col": col,
                    "val": val,
                    "synonyms": {"equals": equals, "prefix": prefix, "contains": contains},
                }
            )
        else:
            eq_filters.append({"col": col, "val": val})

    token_groups, token_operator, token_reason = _extract_fts_groups(question, explicit_cols)
    should_enable_fts = full_text_flag or bool(token_groups)
    groups = token_groups
    if not groups and question:
        groups = [[question]]

    engine_name = settings.get_fts_engine()
    fts_engine = resolve_engine(engine_name)
    raw_min_len = settings.get_with_global("DW_FTS_MIN_TOKEN_LEN", 2)
    try:
        min_token_len = max(1, int(raw_min_len))
    except (TypeError, ValueError):
        min_token_len = 2

    contract_table = settings.get_with_global("DW_CONTRACT_TABLE", "Contract")
    date_column = settings.get_with_global("DW_DATE_COLUMN", "REQUEST_DATE")

    qb = QueryBuilder(table=contract_table, date_col=date_column)
    qb.wants_all_columns(True)

    if should_enable_fts and fts_columns:
        qb.use_fts(engine=fts_engine, columns=fts_columns, min_token_len=min_token_len)
        for group in groups:
            qb.add_fts_group(group, op=token_operator)

    if eq_filters:
        qb.apply_eq_filters(eq_filters)

    qb.order_by(date_column, desc=True)

    final_sql, binds = qb.compile()
    logger.info(json.dumps({"final_sql": {"size": len(final_sql), "sql": final_sql}}))
    rows = fetch_rows(final_sql, binds)

    flat_tokens = _flatten(token_groups) if token_groups else []
    fts_bind_names = [name for name in binds if name.startswith("fts_")]
    fts_enabled = should_enable_fts and bool(fts_columns) and bool(fts_bind_names)
    fts_reason = token_reason if (fts_enabled or flat_tokens) else None
    explain_parts: List[str] = []
    if fts_enabled:
        cols_list = ", ".join(str(col) for col in fts_columns) or "(no columns configured)"
        explain_parts.append(
            f"FTS tokens joined with {token_operator} ({token_reason}). Columns: {cols_list}."
        )
    if eq_applied:
        cols = ", ".join(item["col"] for item in eq_applied)
        explain_parts.append(f"Equality filters applied on {cols}.")

    builder_notes = qb.debug_info().get("notes")

    debug = {
        "fts": {
            "enabled": bool(fts_enabled),
            "tokens": flat_tokens if fts_enabled else None,
            "columns": fts_columns if fts_enabled else None,
            "binds": fts_bind_names or None,
            "engine": engine_name,
            "reason": fts_reason,
        },
        "intent": {
            "full_text_search": bool(fts_enabled),
            "fts_tokens": flat_tokens,
            "fts_operator": token_operator if fts_enabled else None,
            "eq_filters": eq_applied,
        },
        "explain": explain_parts,
        "final_sql": {"sql": final_sql, "size": len(final_sql)},
        "builder_notes": builder_notes,
    }

    meta = {
        "binds": binds,
        "strategy": "fts_like" if fts_enabled else ("eq_only" if eq_filters else "deterministic"),
        "fts": {
            "enabled": bool(fts_enabled),
            "mode": "explicit" if fts_enabled else None,
            "columns": fts_columns if fts_enabled else [],
            "binds": fts_bind_names,
            "engine": engine_name,
            "operator": token_operator if fts_enabled else None,
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

    date_column = raw_settings.get("DW_DATE_COLUMN") or "REQUEST_DATE"
    fts_engine_obj = resolve_engine(fts_engine)
    min_token_len = fts_cfg.get("min_token_len", 2)

    eq_filters_raw = hints.get("eq_filters") or []
    eq_filters = _inject_request_type_synonyms(eq_filters_raw, settings)
    intent = {
        "date_column": "OVERLAP",
        "fts_tokens": tokens,
        "fts_operator": operator,
        "full_text_search": bool(tokens),
        "eq_filters": eq_filters,
        "boolean_groups": hints.get("boolean_groups") or [],
        "group_by": group_col,
        "gross": gross_flag,
        "sort_by": sort_by_hint,
        "sort_desc": sort_desc_hint,
        "top_n": top_n,
        "direction_hint": direction_hint,
        "wants_all_columns": True,
    }

    qb = QueryBuilder(table=contract_table, date_col=date_column)
    qb.wants_all_columns(True)

    if tokens and fts_columns:
        qb.use_fts(engine=fts_engine_obj, columns=fts_columns, min_token_len=min_token_len)
        for token in tokens:
            qb.add_fts_group([token], op=operator)

    if intent["eq_filters"]:
        qb.apply_eq_filters(intent["eq_filters"])
    elif intent["boolean_groups"]:
        qb.apply_boolean_groups(intent["boolean_groups"])

    if group_col:
        qb.group_by([group_col], gross=bool(gross_flag))

    sort_desc_effective = sort_desc_hint
    extra_notes: List[str] = []
    if direction_hint is not None and sort_desc_effective is None:
        sort_desc_effective, note = direction_from_words([direction_hint])
        extra_notes.append(note)
    if sort_desc_effective is None:
        sort_desc_effective = True

    if group_cols:
        order_col = sort_by_hint or ("TOTAL_GROSS" if gross_flag else "CNT")
    else:
        order_col = sort_by_hint or date_column or "REQUEST_DATE"

    qb.order_by(order_col, desc=bool(sort_desc_effective))
    qb.limit(top_n)

    final_sql, binds = qb.compile()
    final_sql_to_run = final_sql

    trace_id = request.headers.get("X-Trace-Id") or uuid.uuid4().hex
    logger.info({"event": "rate.primary.start", "trace_id": trace_id})
    logger.info(
        {
            "event": "rate.primary.sql",
            "trace_id": trace_id,
            "sql_preview": final_sql_to_run[:300],
            "binds": scrub_binds(binds),
        }
    )

    rows = fetch_rows(final_sql_to_run, binds)
    rowcount = len(rows)
    logger.info({"event": "rate.primary.done", "trace_id": trace_id, "rowcount": rowcount})

    allow_alt_retry = not env_flag("DW_RATE_DISABLE_ALT_RETRY", False)
    if rowcount == 0 and not allow_alt_retry:
        logger.info({"event": "rate.alt.skip", "trace_id": trace_id, "reason": "disabled"})

    fts_bind_names = [name for name in binds if name.startswith("fts_")]

    intent_debug = dict(intent)
    intent_debug["sort_by_effective"] = order_col
    intent_debug["sort_desc_effective"] = bool(sort_desc_effective)

    builder_notes = (qb.debug_info().get("notes") or []) + extra_notes

    debug = {
        "fts": {
            "enabled": bool(tokens),
            "tokens": tokens or None,
            "columns": fts_columns if tokens else None,
            "binds": fts_bind_names or None,
            "engine": fts_engine,
            "operator": operator if tokens else None,
            "error": fts_cfg.get("error"),
            "min_token_len": min_token_len,
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
        "builder_notes": builder_notes,
        "final_sql": {"sql": final_sql_to_run, "size": len(final_sql_to_run)},
    }

    meta = {
        "attempt_no": 1,
        "binds": binds,
        "fts": {
            "enabled": bool(tokens),
            "engine": fts_engine,
            "operator": operator if tokens else None,
            "columns": fts_columns if tokens else [],
            "error": fts_cfg.get("error"),
            "min_token_len": min_token_len,
        },
        "clarifier_intent": intent_debug,
        "allow_alt_retry": allow_alt_retry,
        "trace_id": trace_id,
        "rowcount": rowcount,
    }

    response = {
        "ok": True,
        "inquiry_id": inquiry_id,
        "sql": final_sql_to_run,
        "debug": debug,
        "meta": meta,
        "rows": rows,
        "retry": False,
    }
    return jsonify(response)


__all__ = ["bp", "answer", "rate"]
