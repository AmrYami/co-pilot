from __future__ import annotations

import json
import os
import time
from typing import Any, Dict, Tuple

try:  # pragma: no cover - allow import without Flask in unit tests
    from flask import current_app
except Exception:  # pragma: no cover
    current_app = None  # type: ignore[assignment]

try:  # pragma: no cover - optional dependency in tests
    from sqlalchemy import text
except Exception:  # pragma: no cover
    def text(sql: str):  # type: ignore
        return sql

from .builder import build_sql
from .intent import NLIntent, parse_intent
from .search import (
    build_fulltext_where,
    extract_search_tokens,
    inject_fulltext_where,
    is_fulltext_allowed,
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
) -> Dict[str, Any]:
    app = current_app
    logger = getattr(app, "logger", None)
    intent: NLIntent = parse_intent(question)
    allow_fts = is_fulltext_allowed()
    if allow_fts:
        default_on = env_flag("DW_FTS_DEFAULT_ON", False)
        if full_text_search is None:
            intent.full_text_search = default_on
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
            tokens = extract_search_tokens(question)
            if tokens:
                table_name = os.getenv("DW_FTS_TABLE", "Contract")
                schema = os.getenv("DW_FTS_SCHEMA") or None
                predicate, fts_binds, columns = build_fulltext_where(
                    engine, table_name, tokens, schema=schema
                )
                if predicate:
                    sql = inject_fulltext_where(sql, predicate)
                    binds.update(fts_binds)
                    fts_meta.update(
                        {
                            "tokens": tokens,
                            "columns": columns,
                            "binds": list(fts_binds.keys()),
                        }
                    )
            else:
                fts_meta["error"] = "no_tokens"
        except Exception as exc:  # pragma: no cover - defensive guard
            fts_meta["error"] = str(exc)
    elif intent.full_text_search and engine is None:
        fts_meta["error"] = "no_engine"

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
        },
    }
    return result
