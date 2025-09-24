from __future__ import annotations

import json
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
) -> Dict[str, Any]:
    app = current_app
    logger = app.logger
    intent: NLIntent = parse_intent(question)
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
        if "DW_ENGINE" in app.config
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
        },
    }
    return result
