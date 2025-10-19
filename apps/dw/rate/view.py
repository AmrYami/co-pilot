from datetime import date, datetime
import logging
from typing import Any, Dict, List, Optional

from flask import Blueprint, current_app, jsonify, request

from apps.dw.feedback_store import persist_feedback
from apps.dw.sql_builder import build_rate_sql
from apps.dw.settings import get_setting, get_settings, load_settings
from apps.dw.learning import record_feedback, to_patch_from_comment
from apps.dw.search import (
    build_fulltext_where,
    extract_fts_tokens,
    get_fts_columns,
    get_fts_engine,
)
from apps.dw.search.filters import build_eq_where
from apps.dw.fts_utils import DEFAULT_CONTRACT_FTS_COLUMNS
from apps.dw.sql.builder import build_eq_boolean_groups_where, normalize_order_by
from apps.dw.rate_dates import build_date_clause

rate_bp = Blueprint("rate", __name__)

log = logging.getLogger("dw")


def _get_auth_email_from_ctx_or_default(req, settings):
    payload: Dict[str, Any] = {}
    try:
        payload = req.get_json(force=True, silent=True) or {}
    except Exception:
        payload = {}

    auth_email = payload.get("auth_email")
    if auth_email:
        return str(auth_email)

    header_email = req.headers.get("X-Auth-Email")
    if header_email:
        return header_email

    fallback_settings = settings or {}
    return str(fallback_settings.get("AUTH_EMAIL", ""))


@rate_bp.route("/dw/rate", methods=["POST"])
def rate():
    payload = request.get_json(force=True, silent=True) or {}
    inquiry_id = payload.get("inquiry_id")
    rating = payload.get("rating")
    comment = (payload.get("comment") or "").strip()
    record_feedback(inquiry_id=inquiry_id, rating=rating, comment=comment)

    patch = None
    if rating is not None and int(rating) <= 2 and comment:
        patch = to_patch_from_comment(comment)

    resp = {"ok": True, "inquiry_id": inquiry_id, "debug": {}}

    settings_store = current_app.config.get("SETTINGS_STORE") if current_app else None
    store_settings: Dict[str, Any] = {}
    if settings_store is not None:
        try:
            loaded = load_settings(settings_store)
            if isinstance(loaded, dict):
                store_settings = loaded
        except Exception:
            store_settings = {}

    effective_settings: Dict[str, Any] = {}
    if isinstance(store_settings, dict):
        effective_settings.update(store_settings)

    def _ensure_setting(key: str, default: Any = None) -> Any:
        if key in effective_settings and effective_settings[key] is not None:
            return effective_settings[key]
        value = get_setting(key, scope="namespace")
        if value is None:
            value = default
        if value is not None:
            effective_settings[key] = value
        return value

    _ensure_setting("DW_FTS_ENGINE", "like")
    _ensure_setting("DW_FTS_COLUMNS", {})
    alias_map_raw = _ensure_setting("DW_EQ_ALIAS_COLUMNS", {}) or {}
    enum_synonyms_setting = _ensure_setting("DW_ENUM_SYNONYMS", {}) or {}
    raw_min_len = _ensure_setting("DW_FTS_MIN_TOKEN_LEN", 2)
    contract_table = str(_ensure_setting("DW_CONTRACT_TABLE", "Contract") or "Contract")
    order_column = str(_ensure_setting("DW_DATE_COLUMN", "REQUEST_DATE") or "REQUEST_DATE")

    try:
        min_token_len = max(1, int(raw_min_len)) if raw_min_len is not None else 2
    except (TypeError, ValueError):
        min_token_len = 2
    effective_settings["DW_FTS_MIN_TOKEN_LEN"] = min_token_len

    engine_name = get_fts_engine(effective_settings)
    configured_columns = get_fts_columns(contract_table, effective_settings)
    search_columns = configured_columns or DEFAULT_CONTRACT_FTS_COLUMNS

    if patch:
        intent = {
            "eq_filters": patch.get("eq_filters") or [],
            "fts": {
                "enabled": bool(patch.get("fts_tokens")),
                "operator": patch.get("fts_operator") or "OR",
                "tokens": [[t] for t in (patch.get("fts_tokens") or [])],
                "columns": search_columns,
                "engine": engine_name,
                "min_token_len": min_token_len,
            },
            "group_by": patch.get("group_by"),
            "sort_by": patch.get("sort_by"),
            "sort_desc": patch.get("sort_desc"),
            "top_n": patch.get("top_n"),
            "gross": patch.get("gross"),
        }
    else:
        intent = {
            "eq_filters": [],
            "fts": {
                "enabled": False,
                "operator": "OR",
                "tokens": [],
                "columns": search_columns,
                "engine": engine_name,
                "min_token_len": min_token_len,
            },
            "group_by": None,
            "sort_by": "REQUEST_DATE",
            "sort_desc": True,
            "top_n": None,
            "gross": None,
        }

    intent["comment"] = comment

    intent_fts = intent.get("fts", {})
    intent_fts["columns"] = search_columns
    intent_fts["engine"] = engine_name
    intent_fts["min_token_len"] = min_token_len
    intent["fts"] = intent_fts

    raw_groups: List[List[str]] = []
    if patch and isinstance(patch.get("fts_groups"), list):
        for group in patch.get("fts_groups") or []:
            if isinstance(group, list):
                cleaned_group = [str(token).strip() for token in group if str(token or "").strip()]
                if cleaned_group:
                    raw_groups.append(cleaned_group)
    if not raw_groups:
        for group in intent_fts.get("tokens") or []:
            if isinstance(group, list):
                cleaned_group = [str(token).strip() for token in group if str(token or "").strip()]
                if cleaned_group:
                    raw_groups.append(cleaned_group)

    filtered_groups = extract_fts_tokens(raw_groups, min_len=min_token_len)
    fts_operator = intent_fts.get("operator") or "OR"
    intent_fts["tokens"] = filtered_groups
    intent_fts["enabled"] = bool(filtered_groups)

    fts_where, fts_binds, fts_error = build_fulltext_where(
        search_columns,
        filtered_groups,
        engine=engine_name,
        operator=fts_operator,
    )

    alias_map = alias_map_raw if isinstance(alias_map_raw, dict) else {}
    eq_filters: List[Dict[str, Any]] = intent.get("eq_filters") or []

    log.info(
        "rate.intent.parsed",
        extra={
            "payload": {
                "fts_groups": filtered_groups,
                "eq_filters": [
                    {"col": f.get("col"), "op": f.get("op", "eq")}
                    for f in (eq_filters or [])
                    if isinstance(f, dict)
                ],
                "sort_by": intent.get("sort_by"),
                "sort_desc": intent.get("sort_desc"),
            }
        },
    )
    request_type_synonyms = enum_synonyms_setting.get("Contract.REQUEST_TYPE", {})
    eq_sql, eq_binds, _ = build_eq_where(
        eq_filters,
        alias_map,
        bind_prefix="eq_",
        start_index=0,
        request_type_synonyms=request_type_synonyms,
    )

    date_intent = None
    date_sql: Optional[str] = None
    date_binds: Dict[str, Any] = {}
    date_debug: Dict[str, Any] = {}
    try:
        date_intent, date_sql, date_binds, date_debug = build_date_clause(
            comment, effective_settings
        )
    except Exception:
        date_intent, date_sql, date_binds, date_debug = None, None, {}, {}

    boolean_groups: List[Dict[str, Any]] = []
    if patch and isinstance(patch.get("boolean_groups"), list):
        for group in patch.get("boolean_groups") or []:
            if isinstance(group, dict):
                boolean_groups.append(group)

    bg_where, bg_binds, _ = build_eq_boolean_groups_where(
        boolean_groups,
        bind_prefix="eq_bg",
        start_index=0,
    )

    where_parts: List[str] = []
    if fts_where:
        where_parts.append(fts_where)
    if bg_where:
        where_parts.append(bg_where)
    if eq_sql:
        where_parts.append(eq_sql)
    if date_sql:
        where_parts.append(f"({date_sql})")

    where_sql = f" WHERE {' AND '.join(where_parts)}" if where_parts else ""
    has_explicit_sort = bool(
        patch
        and (
            patch.get("sort_by") is not None
            or patch.get("order_by") is not None
            or patch.get("sort_desc") is not None
        )
    )
    order_clause = normalize_order_by(
        intent.get("order_by"), intent.get("sort_by"), intent.get("sort_desc")
    )
    if date_intent and getattr(date_intent, "order_by_override", None) and not has_explicit_sort:
        order_clause = date_intent.order_by_override
    final_sql = f'SELECT * FROM "{contract_table}"{where_sql} ORDER BY {order_clause}'

    binds: Dict[str, Any] = {}
    binds.update(date_binds)
    binds.update(fts_binds)
    binds.update(bg_binds)
    binds.update(eq_binds)

    if filtered_groups and not search_columns:
        fts_error = "no_columns"
    elif not filtered_groups and (patch and patch.get("fts_tokens")):
        fts_error = "no_tokens"
    elif filtered_groups and not fts_where and fts_error is None:
        fts_error = "no_predicate"

    enum_syn = request_type_synonyms
    legacy_sql: Optional[str] = None
    legacy_binds: Dict[str, Any] = {}
    try:
        legacy_sql, legacy_binds = build_rate_sql(intent, enum_syn=enum_syn)
    except Exception:
        legacy_sql = None
        legacy_binds = {}

    def _serialize_bind(value: Any) -> Any:
        if isinstance(value, (date, datetime)):
            return value.isoformat()
        return value

    date_debug_payload: Optional[Dict[str, Any]] = None
    if date_sql and date_intent:
        date_debug_payload = {
            "mode": getattr(date_intent, "mode", None),
            "column": getattr(date_intent, "column", None),
            "sql": date_sql,
            "binds": {k: _serialize_bind(v) for k, v in date_binds.items()},
            "input": getattr(date_intent, "input_text", None),
            "order_override": getattr(date_intent, "order_by_override", None),
            "raw": date_debug,
        }
    elif date_sql:
        date_debug_payload = {
            "sql": date_sql,
            "binds": {k: _serialize_bind(v) for k, v in date_binds.items()},
            "raw": date_debug,
        }

    debug_payload = {
        "intent": intent,
        "rate_hints": patch or {},
        "validation": {
            "ok": True,
            "binds": list(binds.keys()),
            "bind_names": list(binds.keys()),
            "errors": [],
        },
        "fts": {
            "enabled": bool(filtered_groups),
            "engine": engine_name,
            "columns": search_columns,
            "groups": filtered_groups,
            "operator": fts_operator,
            "binds": list(fts_binds.keys()),
            "error": fts_error,
        },
        "boolean_groups": {
            "groups": boolean_groups,
            "sql": bg_where,
            "binds": list(bg_binds.keys()),
        },
        "eq": {
            "filters": eq_filters,
            "aliases": alias_map,
            "sql": eq_sql,
            "binds": list(eq_binds.keys()),
        },
        "final_sql": {"size": len(final_sql), "sql": final_sql},
    }
    if date_debug_payload:
        debug_payload["date_window"] = date_debug_payload
    if legacy_sql:
        debug_payload["legacy_sql"] = {
            "sql": legacy_sql,
            "binds": list((legacy_binds or {}).keys()),
        }

    resp.update(
        {
            "retry": True,
            "sql": final_sql,
            "debug": debug_payload,
            "meta": {
                "attempt_no": 2,
                "binds": binds,
                "clarifier_intent": intent,
                "strategy": "det_overlaps_gross",
                "wants_all_columns": True,
                "legacy_sql": legacy_sql,
            },
        }
    )

    resp["binds"] = binds
    resp.setdefault("debug", {}).setdefault("final_sql", {}).setdefault("binds", binds)

    try:
        settings = get_settings()
    except Exception:
        settings = {}

    auth_email = _get_auth_email_from_ctx_or_default(request, settings)

    try:
        inquiry_id_value = int(inquiry_id) if inquiry_id is not None else None
    except (TypeError, ValueError):
        inquiry_id_value = None

    persist_result: Dict[str, Any]
    if inquiry_id_value:
        try:
            persist_feedback_id = persist_feedback(
                inquiry_id=inquiry_id_value,
                auth_email=auth_email,
                rating=int(rating or 0) if rating is not None else 0,
                comment=comment,
                intent=resp.get("debug", {}).get("intent") or intent,
                resolved_sql=resp.get("sql")
                or resp.get("debug", {}).get("final_sql", {}).get("sql"),
                binds=resp.get("binds") or {},
            )
            persist_result = {"ok": True, "feedback_id": persist_feedback_id}
        except Exception as exc:
            persist_result = {"ok": False, "error": str(exc)}
    else:
        persist_result = {"ok": False, "error": "missing_inquiry_id"}
    resp.setdefault("debug", {}).setdefault("persist", persist_result)

    current_app.logger.info(
        {
            "event": "rate.persist",
            "inquiry_id": inquiry_id_value,
            "rating": rating,
            "ok": persist_result.get("ok"),
            "error": persist_result.get("error"),
        }
    )

    return jsonify(resp), 200
