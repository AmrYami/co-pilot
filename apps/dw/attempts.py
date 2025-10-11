from __future__ import annotations

import json
import os
import time
from typing import Any, Dict, Optional, Tuple

try:  # pragma: no cover - allow import without Flask in unit tests
    from flask import current_app
except Exception:  # pragma: no cover
    current_app = None  # type: ignore[assignment]

try:  # pragma: no cover - optional dependency in tests
    from sqlalchemy import text
except Exception:  # pragma: no cover
    def text(sql: str):  # type: ignore
        return sql

from apps.dw.rate_grammar import (
    merge_rate_comment_hints,
    parse_rate_comment_strict,
)
from apps.dw.settings_defaults import DEFAULT_EXPLICIT_FILTER_COLUMNS
from apps.dw.settings_utils import load_explicit_filter_columns

from .builder import build_sql
from .intent import NLIntent, parse_intent_legacy
from .rate_hints import (
    append_where,
    apply_rate_hints,
    parse_rate_hints,
    replace_or_add_order_by,
)
from .search import (
    build_fulltext_where,
    extract_search_tokens,
    inject_fulltext_where,
    is_fulltext_allowed,
    resolve_fts_config,
)
from .utils import env_flag


def _execute_sql(engine, sql: str, binds: Dict[str, Any]) -> Tuple[list, list]:
    with engine.connect() as cx:
        rs = cx.execute(text(sql), binds)
        cols = list(rs.keys()) if hasattr(rs, "keys") else []
        rows = [list(r) for r in rs.fetchall()]
    return rows, cols


def _log(app_logger, tag: str, payload: Dict[str, Any]):
    try:
        app_logger.info("[%s] %s", "dw", json.dumps({tag: payload}, ensure_ascii=False))
    except Exception:
        pass


def run_attempt(
    question: str,
    namespace: str,
    attempt_no: int,
    strategy: str = "deterministic",
    full_text_search: bool | None = None,
    rate_comment: Optional[str] = None,
) -> Dict[str, Any]:
    app = current_app
    logger = getattr(app, "logger", None)
    intent: NLIntent = parse_intent_legacy(question)

    patched: Optional[Dict[str, Any]] = None
    if rate_comment:
        patched = apply_rate_hints(intent.dict(), rate_comment)
        if "eq_filters" in patched:
            intent.eq_filters = patched["eq_filters"]
        for key in ("group_by", "sort_by", "sort_desc", "agg", "gross"):
            if key in patched:
                setattr(intent, key, patched[key])
        if "full_text_search" in patched:
            intent.full_text_search = bool(patched["full_text_search"])
        if "fts_tokens" in patched and patched.get("fts_tokens") is not None:
            intent.fts_tokens = patched["fts_tokens"]
        if "fts_operator" in patched and patched.get("fts_operator"):
            intent.fts_operator = patched["fts_operator"]
        if "fts_columns" in patched and patched.get("fts_columns"):
            intent.fts_columns = patched["fts_columns"]
    allow_fts = is_fulltext_allowed()
    if allow_fts:
        default_on = env_flag("DW_FTS_DEFAULT_ON", False)
        # Respect an explicit FTS signal coming from /dw/rate.
        rate_forced_fts = bool(
            patched and (patched.get("full_text_search") or patched.get("fts_tokens"))
        )
        current_fts = getattr(intent, "full_text_search", None)
        if full_text_search is None:
            # If /dw/rate did not force FTS and the intent does not have an explicit value, use default.
            if not rate_forced_fts and current_fts is None:
                intent.full_text_search = default_on
            # Otherwise keep the value already set on the intent.
        else:
            intent.full_text_search = bool(full_text_search)
    else:
        intent.full_text_search = False
    if strategy == "det_overlaps_gross":
        intent.measure_sql = (
            "NVL(CONTRACT_VALUE_NET_OF_VAT,0) + "
            "CASE WHEN NVL(VAT,0) BETWEEN 0 AND 1 "
            "THEN NVL(CONTRACT_VALUE_NET_OF_VAT,0) * NVL(VAT,0) "
            "ELSE NVL(VAT,0) END"
        )
        if intent.has_time_window and not intent.expire:
            intent.date_column = None

    app_config = getattr(app, "config", {}) if app else {}
    pipeline = None
    if hasattr(app_config, "get"):
        pipeline = app_config.get("PIPELINE") or app_config.get("pipeline")
    elif isinstance(app_config, dict):
        pipeline = app_config.get("PIPELINE") or app_config.get("pipeline")
    settings_obj = getattr(pipeline, "settings", None) if pipeline else None
    settings_getter = None
    if settings_obj is not None:
        settings_getter = getattr(settings_obj, "get_json", None) or getattr(settings_obj, "get", None)
    allowed_columns = load_explicit_filter_columns(
        settings_getter, namespace, DEFAULT_EXPLICIT_FILTER_COLUMNS
    )

    def _settings_get(key: str, default: Any = None) -> Any:
        if not callable(settings_getter):
            return default
        candidates = (
            {"scope": "namespace", "namespace": namespace},
            {"scope": "namespace"},
            {},
        )
        for kwargs in candidates:
            try:
                value = settings_getter(key, **kwargs)
            except TypeError:
                continue
            if value is not None:
                return value
        return default

    def _unwrap_setting(value: Any, default: Any) -> Any:
        if isinstance(value, dict) and "value" in value and len(value) == 1:
            return value.get("value", default)
        return value if value is not None else default

    namespace_settings: Dict[str, Any] = {
        "DW_FTS_ENGINE": _unwrap_setting(
            _settings_get("DW_FTS_ENGINE", "like"), "like"
        ),
        "DW_FTS_COLUMNS": _unwrap_setting(_settings_get("DW_FTS_COLUMNS", {}), {}),
        "DW_FTS_MIN_TOKEN_LEN": _unwrap_setting(
            _settings_get("DW_FTS_MIN_TOKEN_LEN", 2), 2
        ),
        "DW_EQ_ALIAS_COLUMNS": _unwrap_setting(
            _settings_get("DW_EQ_ALIAS_COLUMNS", {}), {}
        ),
    }

    fts_conf = resolve_fts_config(namespace_settings)

    strict_hints = parse_rate_comment_strict(rate_comment)
    if rate_comment and not strict_hints.is_empty():
        intent = merge_rate_comment_hints(intent, strict_hints, allowed_columns)
    sql, binds = build_sql(intent)

    fts_meta: Dict[str, Any] = {
        "enabled": bool(intent.full_text_search),
        "tokens": None,
        "columns": None,
        "binds": None,
        "error": None,
    }

    engine = app.config.get("DW_ENGINE") if app else None
    if intent.full_text_search and engine is not None:
        try:
            min_len = int(fts_conf.get("min_len", 2) or 2)
            tokens = extract_search_tokens(question, min_len=min_len)
            if tokens:
                table_name = os.getenv("DW_FTS_TABLE", "Contract")
                columns_map = fts_conf.get("fts_columns_map") or {}
                columns = (
                    columns_map.get(table_name)
                    or columns_map.get(table_name.upper())
                    or columns_map.get(table_name.strip('"'))
                    or columns_map.get("*")
                    or []
                )
                groups = [[token] for token in tokens]
                predicate, fts_binds = build_fulltext_where(
                    groups=groups,
                    columns=columns,
                    engine=fts_conf.get("engine", "like"),
                    min_len=min_len,
                    bind_prefix="fts_",
                )
                if predicate:
                    sql = inject_fulltext_where(sql, predicate)
                    binds.update(fts_binds)
                    fts_meta.update(
                        {
                            "tokens": tokens,
                            "columns": columns,
                            "binds": list(fts_binds.keys()),
                            "engine": fts_conf.get("engine"),
                        }
                    )
                else:
                    fts_meta["error"] = "no_columns" if not columns else "no_predicate"
            else:
                fts_meta["error"] = "no_tokens"
        except Exception as exc:  # pragma: no cover - defensive guard
            fts_meta["error"] = str(exc)
    elif intent.full_text_search and engine is None:
        fts_meta["error"] = "no_engine"

    hints_meta = {
        "comment_present": bool(rate_comment and rate_comment.strip()),
        "where_applied": bool(strict_hints.filters),
        "order_by_applied": bool(strict_hints.order_by),
        "group_by": list(strict_hints.group_by) if strict_hints.group_by else None,
        "eq_filters": len(getattr(intent, "eq_filters", [])),
    }

    if sql and rate_comment and rate_comment.strip():
        hints = parse_rate_hints(rate_comment, settings_getter)
        if hints.where_sql:
            sql = append_where(sql, hints.where_sql)
            binds.update(hints.where_binds)
            hints_meta["where_applied"] = True
        if hints.order_by_sql:
            sql = replace_or_add_order_by(sql, hints.order_by_sql)
            hints_meta["order_by_applied"] = True
        if hints.group_by_cols:
            hints_meta["group_by"] = list(hints.group_by_cols)

    _log(
        logger,
        "fts",
        {
            "enabled": fts_meta["enabled"],
            "tokens": fts_meta.get("tokens"),
            "columns": fts_meta.get("columns"),
            "binds": fts_meta.get("binds"),
            "error": fts_meta.get("error"),
        },
    )
    _log(logger, "final_sql", {"size": len(sql), "sql": sql})
    _log(
        logger,
        "validation",
        {"ok": True, "errors": [], "binds": list(binds.keys()), "bind_names": list(binds.keys())},
    )

    if env_flag("DW_SLOW_ACCURATE", True):
        time.sleep(0.15)
    rows, cols = (
        _execute_sql(app.config["DW_ENGINE"], sql, binds)
        if app and "DW_ENGINE" in app.config
        else ([], [])
    )

    result = {
        "ok": True,
        "sql": sql,
        "rows": rows,
        "columns": cols,
        "meta": {
            "binds": binds,
            "wants_all_columns": intent.wants_all_columns,
            "clarifier_intent": intent.__dict__,
            "rowcount": len(rows),
            "attempt_no": attempt_no,
            "strategy": strategy,
            "fts": fts_meta,
            "rate_hints": hints_meta,
        },
        "debug": {
            "intent": intent.__dict__,
            "prompt": "",
            "validation": {
                "ok": True,
                "errors": [],
                "binds": list(binds.keys()),
                "bind_names": list(binds.keys()),
            },
            "fts": fts_meta,
            "rate_hints": hints_meta,
        },
    }
    return result
