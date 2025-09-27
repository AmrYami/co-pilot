import re
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


from .contracts import build_sql_for_intent, parse_contract_intent
from .rating import rate_bp
from .util import compose_explain, ensure_oracle_date_binds, get_fts_columns


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
        safe_binds = ensure_oracle_date_binds(binds)
        rs = cx.execute(text(sql), safe_binds)
        cols = list(rs.keys()) if hasattr(rs, "keys") else []
        rows = [list(r) for r in rs.fetchall()]
    return rows, cols, {"ms": 0}


@dw_bp.post("/answer")
def answer():
    payload = request.get_json(force=True) or {}
    question = (payload.get("question") or "").strip()
    if not question:
        return jsonify({"ok": False, "error": "question required"}), 400

    auth_email = payload.get("auth_email")  # kept for parity, not used yet
    _ = auth_email  # pragma: no cover - reserved for future auditing
    full_text_search = bool(payload.get("full_text_search", False))

    pipeline = current_app.config.get("PIPELINE") or current_app.config.get("pipeline")
    settings = getattr(pipeline, "settings", {}) if pipeline else {}
    settings_get = getattr(settings, "get", None)
    if callable(settings_get):
        table = settings_get("DW_CONTRACT_TABLE", "Contract")
    elif isinstance(settings, dict):
        table = settings.get("DW_CONTRACT_TABLE", "Contract")
    else:
        table = "Contract"

    # 1) Parse intent (Contract domain)
    intent = parse_contract_intent(question)
    intent.full_text_search = full_text_search
    if full_text_search:
        tokens = [tok for tok in re.split(r"\W+", question) if len(tok) >= 3]
        intent.fts_tokens = tokens

    # 2) Default top_n if user requested Top without number → 10
    if intent.user_requested_top_n and not intent.top_n:
        intent.top_n = 10

    # 3) Prepare binds (dates/limits)
    binds: Dict[str, Any] = {}
    if intent.explicit_dates:
        binds["date_start"] = intent.explicit_dates.get("start")
        binds["date_end"] = intent.explicit_dates.get("end")
    if intent.top_n:
        binds["top_n"] = intent.top_n

    # 4) FTS columns from settings
    settings_for_fts = settings if hasattr(settings, "get") else {}
    fts_cols = get_fts_columns(settings_for_fts, table) if full_text_search else []

    # 5) Build SQL
    sql, extra_binds = build_sql_for_intent(intent, table_name=table, fts_cols=fts_cols)
    binds.update(extra_binds)

    # 6) Execute
    rows, cols, meta = _execute_oracle(sql, binds)

    # 7) Explain text
    explain = compose_explain(intent, binds)

    out_meta = {
        "binds": {k: str(v) for k, v in binds.items()},
        "rowcount": len(rows),
    }
    out_meta.update(meta or {})

    response = {
        "ok": True,
        "inquiry_id": None,
        "sql": sql,
        "rows": rows,
        "columns": cols,
        "meta": out_meta,
        "explain": explain,
    }
    return jsonify(response)


def create_dw_blueprint(*args, **kwargs):
    return dw_bp
