"""DocuWare DW blueprint backed by a deterministic contract planner."""
from __future__ import annotations

import logging
import re
import time
from datetime import date, timedelta
from typing import Any, Dict, List, Optional, Sequence, Tuple

try:  # pragma: no cover - allow unit tests without Flask dependency
    from flask import Blueprint, current_app, jsonify, request
except Exception:  # pragma: no cover - lightweight stub for tests
    current_app = None  # type: ignore[assignment]

    class _StubBlueprint:
        def __init__(self, *args, **kwargs):
            pass

        def register_blueprint(self, *args, **kwargs):
            return None

        def post(self, *args, **kwargs):
            def decorator(fn):
                return fn

            return decorator

        def get(self, *args, **kwargs):
            def decorator(fn):
                return fn

            return decorator

    def _jsonify(*args, **kwargs):  # pragma: no cover - stub
        return {}

    class _StubRequest:  # pragma: no cover - stub
        args: Dict[str, str] = {}

        def get_json(self, force: bool = False):
            return {}

    Blueprint = _StubBlueprint  # type: ignore[assignment]
    jsonify = _jsonify  # type: ignore[assignment]
    request = _StubRequest()  # type: ignore[assignment]

try:  # pragma: no cover - optional dependency during tests
    from sqlalchemy import text
except Exception:  # pragma: no cover - fallback for tests
    def text(sql: str):  # type: ignore
        return sql

from core.inquiries import create_or_update_inquiry

from .contracts.contract_common import coerce_oracle_binds
from .contracts.contract_planner import plan_contract_query
from .rating import rate_bp

LOGGER = logging.getLogger("dw.app")


dw_bp = Blueprint("dw", __name__)
dw_bp.register_blueprint(rate_bp, url_prefix="")


def _ensure_engine():
    app = current_app
    if app is None:
        return None
    config = getattr(app, "config", {})
    getter = getattr(config, "get", None)
    engine = getter("DW_ENGINE") if callable(getter) else config.get("DW_ENGINE") if isinstance(config, dict) else None
    if engine is not None:
        return engine
    pipeline = None
    if callable(getter):
        pipeline = getter("PIPELINE") or getter("pipeline")
    elif isinstance(config, dict):
        pipeline = config.get("PIPELINE") or config.get("pipeline")
    if pipeline is None:
        return None
    try:
        return pipeline.ds.engine(None)  # type: ignore[attr-defined]
    except Exception:  # pragma: no cover - defensive fallback
        return getattr(pipeline, "app_engine", None)


def _execute_oracle(sql: str, binds: Dict[str, Any]):
    engine = _ensure_engine()
    if engine is None:
        return [], [], {"rows": 0}
    safe_binds = coerce_oracle_binds(binds or {})
    with engine.connect() as cx:  # type: ignore[union-attr]
        rs = cx.execute(text(sql), safe_binds)
        cols = list(rs.keys()) if hasattr(rs, "keys") else []
        rows = [list(r) for r in rs.fetchall()]
    return rows, cols, {"rows": len(rows)}


def _coerce_prefixes(raw: Any) -> List[str]:
    if not raw:
        return []
    if isinstance(raw, (list, tuple, set)):
        return [str(p) for p in raw if p is not None]
    return [str(raw)]


def _dates_to_iso(explicit: Optional[Tuple[date, date]]) -> Optional[Dict[str, str]]:
    if not explicit:
        return None
    start, end = explicit
    return {"start": start.isoformat(), "end": end.isoformat()}


_LAST_DAYS_RE = re.compile(r"last\s+(\d+)\s+day", re.IGNORECASE)
_NEXT_DAYS_RE = re.compile(r"(?:next|in)\s+(\d+)\s+day", re.IGNORECASE)
_YEAR_RE = re.compile(r"\b(?:in|for|during)\s+(20\d{2})\b")
_TOP_RE = re.compile(r"\btop\s+(\d+)\b", re.IGNORECASE)
_TEXTUAL_TOP = {
    "ten": 10,
    "five": 5,
    "three": 3,
    "twenty": 20,
    "twenty five": 25,
    "thirty": 30,
}


def _resolve_window(question: str) -> Optional[Tuple[date, date]]:
    q = (question or "").lower()
    today = date.today()

    if "last month" in q or "previous month" in q:
        first_this = today.replace(day=1)
        last_month_end = first_this - timedelta(days=1)
        last_month_start = last_month_end.replace(day=1)
        return last_month_start, last_month_end

    if "last quarter" in q:
        quarter_start_month = ((today.month - 1) // 3) * 3 + 1
        prev_start_month = quarter_start_month - 3
        prev_year = today.year
        if prev_start_month < 1:
            prev_start_month += 12
            prev_year -= 1
        start = date(prev_year, prev_start_month, 1)
        next_month = prev_start_month + 3
        next_year = prev_year
        if next_month > 12:
            next_month -= 12
            next_year += 1
        end = date(next_year, next_month, 1) - timedelta(days=1)
        return start, end

    match = _LAST_DAYS_RE.search(q)
    if match:
        days = int(match.group(1))
        if days > 0:
            end = today
            start = today - timedelta(days=days)
            return start, end

    match = _NEXT_DAYS_RE.search(q)
    if match:
        days = int(match.group(1))
        if days > 0:
            start = today
            end = today + timedelta(days=days)
            return start, end

    if "this year" in q:
        start = date(today.year, 1, 1)
        end = date(today.year, 12, 31)
        return start, end

    match = _YEAR_RE.search(q)
    if match:
        year = int(match.group(1))
        if 2000 <= year <= 2100:
            start = date(year, 1, 1)
            end = date(year, 12, 31)
            return start, end

    return None


def _get_pipeline():
    app = current_app
    if app is None:
        return None
    config = getattr(app, "config", {})
    getter = getattr(config, "get", None)
    if callable(getter):
        return getter("PIPELINE") or getter("pipeline")
    if isinstance(config, dict):
        return config.get("PIPELINE") or config.get("pipeline")
    return None


def _get_settings():
    pipeline = _get_pipeline()
    if pipeline is None:
        return None
    return getattr(pipeline, "settings", None)


def _get_fts_columns(*, table: str, namespace: str) -> List[str]:
    settings = _get_settings()
    if settings is None:
        return []
    getter = getattr(settings, "get_fts_columns", None)
    if callable(getter):
        return getter(table)  # type: ignore[return-value]
    mapping = settings.get("DW_FTS_COLUMNS", scope="namespace", namespace=namespace) if hasattr(settings, "get") else {}
    if isinstance(mapping, dict):
        return mapping.get(table, mapping.get("*", [])) or []
    return []


def _extract_fts_tokens(question: str) -> List[str]:
    tokens = [tok for tok in re.split(r"\W+", question or "") if len(tok) >= 3]
    return [tok.upper() for tok in tokens]


def _extract_top_n(question: str) -> Optional[int]:
    match = _TOP_RE.search(question or "")
    if match:
        try:
            return max(1, min(int(match.group(1)), 500))
        except ValueError:
            pass
    lowered = (question or "").lower()
    for phrase, number in _TEXTUAL_TOP.items():
        if f"top {phrase}" in lowered:
            return number
    return None


def _coerce_datasource(pipeline, body: Dict[str, Any]) -> str:
    if pipeline is None:
        return body.get("datasource") or "default"
    datasource = body.get("datasource")
    if datasource:
        return str(datasource)
    default = None
    settings = getattr(pipeline, "settings", None)
    getter = getattr(settings, "default_datasource", None)
    if callable(getter):
        default = getter("dw::common")
    if not default:
        default = getattr(pipeline, "default_ds", None)
    return default or "default"


def _log_inquiry(
    question: str,
    auth_email: Optional[str],
    *,
    status: str,
    rows: int,
    prefixes: Sequence[str],
    payload: Dict[str, Any],
) -> Optional[int]:
    pipeline = _get_pipeline()
    if pipeline is None:
        return None
    mem_engine = getattr(pipeline, "mem_engine", None)
    if mem_engine is None:
        return None
    try:
        datasource = _coerce_datasource(pipeline, payload)
        inquiry_id = create_or_update_inquiry(
            mem_engine,
            namespace="dw::common",
            prefixes=list(prefixes),
            question=question,
            auth_email=auth_email,
            run_id=None,
            research_enabled=False,
            datasource=datasource,
            status=status,
        )
        if rows >= 0:
            return inquiry_id
        return inquiry_id
    except Exception as exc:  # pragma: no cover - logging guard
        LOGGER.warning("[dw] failed to log inquiry: %s", exc)
        return None


@dw_bp.post("/answer")
def answer():
    t0 = time.time()
    payload = request.get_json(force=True) or {}
    question = (payload.get("question") or "").strip()
    if not question:
        return jsonify({"ok": False, "error": "question required"}), 400

    prefixes = _coerce_prefixes(payload.get("prefixes"))
    auth_email = payload.get("auth_email") or None
    full_text_search = bool(payload.get("full_text_search") or False)

    explicit_dates = _resolve_window(question)

    fts_columns = _get_fts_columns(table="Contract", namespace="dw::common")
    fts_tokens = _extract_fts_tokens(question) if full_text_search else []

    top_n = payload.get("top_n")
    if top_n is None:
        top_n = _extract_top_n(question)
    elif isinstance(top_n, str) and top_n.isdigit():
        top_n = int(top_n)

    sql, binds, meta, explain = plan_contract_query(
        question,
        explicit_dates=explicit_dates,
        top_n=top_n,
        full_text_search=full_text_search,
        fts_columns=fts_columns,
        fts_tokens=fts_tokens,
    )

    LOGGER.info("[dw] final_sql: %s", {"size": len(sql), "sql": sql})
    rows, cols, exec_meta = _execute_oracle(sql, binds or {})

    inquiry_id = _log_inquiry(
        question,
        auth_email,
        status="answered",
        rows=len(rows),
        prefixes=prefixes,
        payload=payload,
    )

    duration_ms = int((time.time() - t0) * 1000)
    response = {
        "ok": True,
        "inquiry_id": inquiry_id,
        "rows": rows,
        "columns": cols,
        "sql": sql,
        "meta": {**(meta or {}), **exec_meta, "duration_ms": duration_ms},
        "explain": explain,
        "debug": {
            "intent": {
                "explicit_dates": _dates_to_iso(explicit_dates),
                "top_n": top_n,
                "full_text_search": full_text_search,
                "fts_tokens": fts_tokens,
            }
        },
    }
    return jsonify(response)


def create_dw_blueprint(*args, **kwargs):
    return dw_bp
