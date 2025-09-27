import os
import re
import datetime as dt
from typing import Any, Dict, List

try:  # pragma: no cover - allow tests to import without Flask installed
    from flask import Blueprint, current_app, jsonify, request
except Exception:  # pragma: no cover - simple stub used in unit tests
    current_app = None  # type: ignore[assignment]

    class _StubBlueprint:  # minimal methods to satisfy imports
        def __init__(self, *args, **kwargs):
            pass

        def register_blueprint(self, *args, **kwargs):
            return None

        def post(self, *args, **kwargs):
            def decorator(fn):
                return fn

            return decorator

    def _jsonify(*args, **kwargs):  # pragma: no cover - test stub
        return {}

    class _StubRequest:  # pragma: no cover - test stub
        args: Dict[str, str] = {}

        def get_json(self, force: bool = False):  # noqa: D401 - simple stub
            return {}

    Blueprint = _StubBlueprint  # type: ignore[assignment]
    jsonify = _jsonify  # type: ignore[assignment]
    request = _StubRequest()  # type: ignore[assignment]

try:  # pragma: no cover - optional dependency in tests
    from sqlalchemy import text
except Exception:  # pragma: no cover - lightweight fallback used in tests
    def text(sql: str):  # type: ignore
        return sql


import yaml

from .contracts import parse_intent_contract, build_sql_contract, explain_interpretation
from .rating import rate_bp


dw_bp = Blueprint("dw", __name__)
dw_bp.register_blueprint(rate_bp, url_prefix="")


def _resolve_dw_engine(app):
    if app is None:
        return None
    engine = app.config.get("DW_ENGINE") if app else None  # type: ignore[assignment]
    if engine is not None:
        return engine
    pipeline = app.config.get("PIPELINE") if app else None
    if pipeline is None:
        pipeline = app.config.get("pipeline") if app else None
    if pipeline is None:
        return None
    try:
        return pipeline.ds.engine(None)  # type: ignore[attr-defined]
    except Exception:  # pragma: no cover - defensive fallback
        return getattr(pipeline, "app_engine", None)


def _execute_oracle(sql: str, binds: Dict[str, Any]):
    app = current_app
    rows: List[List[Any]] = []
    cols: List[str] = []
    engine = _resolve_dw_engine(app)
    if engine is None:
        return rows, cols, {"ms": 0}
    with engine.connect() as cx:  # type: ignore[union-attr]
        safe_binds = _coerce_date_binds(binds)
        rs = cx.execute(text(sql), safe_binds)
        cols = list(rs.keys()) if hasattr(rs, "keys") else []
        rows = [list(r) for r in rs.fetchall()]
    return rows, cols, {"ms": 0}


def _coerce_date_binds(binds: Dict[str, Any]) -> Dict[str, Any]:
    """Ensure Oracle date binds stay datetime.date objects."""
    out: Dict[str, Any] = {}
    for key, value in (binds or {}).items():
        if key in {"date_start", "date_end"}:
            if isinstance(value, dt.date):
                out[key] = value
            elif isinstance(value, str):
                out[key] = dt.date.fromisoformat(value)
            else:
                out[key] = value
        else:
            out[key] = value
    return out


@dw_bp.post("/answer")
def answer():
    payload = request.get_json(force=True) or {}
    question = (payload.get("question") or "").strip()
    if not question:
        return jsonify({"ok": False, "error": "question required"}), 400

    auth_email = payload.get("auth_email")  # kept for parity, not used yet
    _ = auth_email  # pragma: no cover - reserved for future auditing
    full_text_search = bool(payload.get("full_text_search", False))

    table_name = "Contract"
    app_obj = current_app
    config = getattr(app_obj, "config", None)
    pipeline = None
    if config is not None:
        config_get = getattr(config, "get", None)
        if callable(config_get):
            pipeline = config_get("PIPELINE") or config_get("pipeline")
        elif isinstance(config, dict):
            pipeline = config.get("PIPELINE") or config.get("pipeline")
    settings = getattr(pipeline, "settings", {}) if pipeline else {}
    getter = getattr(settings, "get", None)
    if callable(getter):
        table_name = getter("DW_CONTRACT_TABLE", table_name)
    elif isinstance(settings, dict):
        table_name = settings.get("DW_CONTRACT_TABLE", table_name)

    intent = parse_intent_contract(question, full_text_search=full_text_search)
    intent.table = table_name
    if full_text_search:
        tokens = [tok for tok in re.split(r"\W+", question) if len(tok) >= 3]
        intent.fts_tokens = tokens

    if intent.user_requested_top_n and not intent.top_n:
        intent.top_n = 10

    select_all_default = bool(int(os.getenv("DW_SELECT_ALL_DEFAULT", "1")))
    sql, binds = build_sql_contract(intent, select_all_default=select_all_default)
    binds = _coerce_date_binds(binds)

    rows, cols, meta = _execute_oracle(sql, binds)

    resp: Dict[str, Any] = {
        "ok": True,
        "sql": sql,
        "rows": rows,
        "columns": cols,
        "meta": {**(meta or {}), "clarifier_intent": intent.__dict__},
        "debug": {
            "intent": intent.__dict__,
        },
    }
    if os.getenv("DW_EXPLAIN", "1") == "1":
        resp["explain"] = explain_interpretation(intent)
    return jsonify(resp)


@dw_bp.post("/admin/run_golden")
def run_golden():
    """Execute deterministic golden test suite for contract intents."""
    body = request.get_json(force=True) or {}
    path = body.get("path") or "apps/dw/tests/golden_dw_contracts.yaml"
    with open(path, "r", encoding="utf-8") as handle:
        suite = yaml.safe_load(handle) or {}

    results: List[Dict[str, Any]] = []
    passed = 0
    total = 0
    for case in suite.get("tests", []):
        total += 1
        q = case.get("question", "")
        intent = parse_intent_contract(q, full_text_search=bool(case.get("full_text_search")))
        sql, binds = build_sql_contract(intent, select_all_default=True)
        ok = True
        why: List[str] = []
        expect = case.get("expect", {})
        for token in expect.get("sql_contains", []):
            if token not in sql:
                ok = False
                why.append(f"missing token: {token}")
        if expect.get("must_group_by") and " GROUP BY " not in sql:
            ok = False
            why.append("GROUP BY expected")
        if expect.get("must_order_by") and " ORDER BY " not in sql:
            ok = False
            why.append("ORDER BY expected")
        results.append({
            "question": q,
            "ok": ok,
            "sql": sql,
            "why": why,
            "binds": binds,
        })
        if ok:
            passed += 1
    return jsonify({"ok": True, "passed": passed, "total": total, "results": results})


def create_dw_blueprint(*args, **kwargs):
    return dw_bp
