import json
import logging
import os
from datetime import datetime, timedelta

from flask import Blueprint, current_app, jsonify, request
from logging.handlers import TimedRotatingFileHandler
from sqlalchemy import text

from core.sql_exec import get_mem_engine, get_oracle_engine
from .llm import nl_to_sql_with_llm


dw_bp = Blueprint("dw", __name__, url_prefix="/dw")


def _setup_logging(app):
    if any(getattr(handler, "_dw_log", False) for handler in app.logger.handlers):
        return
    log_dir = os.path.join(os.getcwd(), "logs")
    os.makedirs(log_dir, exist_ok=True)
    fh = TimedRotatingFileHandler(os.path.join(log_dir, "dw.log"), when="midnight", backupCount=7, encoding="utf-8")
    fh.setLevel(logging.INFO)
    fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s"))
    fh._dw_log = True
    app.logger.addHandler(fh)
    app.logger.info("[dw] logging initialized")


@dw_bp.before_app_request
def _before():
    _setup_logging(current_app)


def _log(tag: str, payload):
    try:
        current_app.logger.info(f"[dw] {tag}: {json.dumps(payload, default=str)[:4000]}")
    except Exception:
        current_app.logger.info(f"[dw] {tag}: {payload}")


@dw_bp.route("/answer", methods=["POST"])
def answer():
    body = request.get_json(force=True, silent=True) or {}
    question = (body.get("question") or "").strip()
    auth_email = body.get("auth_email") or ""
    prefixes = body.get("prefixes") or []
    namespace = "dw::common"

    settings = current_app.config.get("SETTINGS")
    if not settings:
        raise RuntimeError("Application settings are not configured")

    mem = get_mem_engine(settings)
    with mem.begin() as conn:
        stmt = text(
            """
            INSERT INTO mem_inquiries(namespace, question, auth_email, prefixes, status, created_at, updated_at)
            VALUES (:ns, :q, :auth, :pfx::jsonb, 'open', NOW(), NOW())
            RETURNING id
            """
        )
        inq_id = conn.execute(
            stmt,
            {"ns": namespace, "q": question, "auth": auth_email, "pfx": json.dumps(prefixes)},
        ).scalar_one()

    _log("inquiry_start", {"id": inq_id, "q": question, "email": auth_email})

    llm_context = {"namespace": namespace, "table": "Contract"}

    out = nl_to_sql_with_llm(question, llm_context)
    debug = out.get("debug") or {}
    intent = out.get("intent") or {}

    clarifier_dbg = debug.get("clarifier") or {}
    if clarifier_dbg.get("raw") is not None:
        _log(
            "clarifier_raw",
            {
                "used": clarifier_dbg.get("used"),
                "ok": clarifier_dbg.get("ok"),
                "raw": (clarifier_dbg.get("raw") or "")[:1200],
            },
        )
    if clarifier_dbg.get("intent") is not None:
        _log("clarifier_intent", clarifier_dbg.get("intent"))

    if debug.get("prompt"):
        _log("sql_prompt", {"prompt": debug["prompt"][:1200]})
    if debug.get("raw1") is not None:
        _log("llm_raw_pass1", {"size": len(debug.get("raw1") or "")})
    if debug.get("raw2") is not None:
        _log("llm_raw_pass2", {"size": len(debug.get("raw2") or "")})

    sql = out.get("sql") or ""

    if not sql:
        _log("validation", {"ok": False, "errors": ["empty_sql"], "binds": []})
        return (
            jsonify(
                {
                    "ok": False,
                    "status": "needs_clarification",
                    "questions": [
                        "I couldn't derive a clean SELECT. Can you rephrase or specify filters (stakeholders, departments, date columns)?"
                    ],
                    "error": "empty_sql",
                    "debug": debug,
                }
            ),
            200,
        )

    binds: dict[str, object] = {}
    question_lower = question.lower()
    if intent.get("has_time_window"):
        now = datetime.utcnow().date()
        if "last month" in question_lower:
            first_this_month = now.replace(day=1)
            date_end = first_this_month
            date_start = (first_this_month - timedelta(days=1)).replace(day=1)
        else:
            date_end = now
            date_start = now - timedelta(days=30)
        binds["date_start"] = datetime.combine(date_start, datetime.min.time())
        binds["date_end"] = datetime.combine(date_end, datetime.min.time())

    _log("final_sql", {"size": len(sql), "sql": sql[:1200]})
    _log(
        "execution_binds",
        {k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in binds.items()},
    )

    engine = get_oracle_engine()
    rows: list[list[object]] = []
    cols: list[str] = []
    started = datetime.utcnow()
    try:
        with engine.begin() as conn:
            result = conn.exec_driver_sql(sql, binds)
            cols = list(result.keys())
            rows = [list(row) for row in result.fetchall()]
        elapsed_ms = int((datetime.utcnow() - started).total_seconds() * 1000)
        _log("execution_result", {"rows": len(rows), "cols": cols, "ms": elapsed_ms})
    except Exception as exc:  # pragma: no cover - surface database errors
        _log("oracle_error", {"error": str(exc)})
        return (
            jsonify(
                {
                    "ok": False,
                    "status": "failed",
                    "error": str(exc),
                    "sql": sql,
                    "binds": binds,
                    "debug": debug,
                }
            ),
            200,
        )

    return (
        jsonify(
            {
                "ok": True,
                "rows": rows,
                "columns": cols,
                "sql": sql,
                "binds": {k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in binds.items()},
                "intent": intent,
                "debug": {
                    "sizes": {
                        "prompt": len(debug.get("prompt") or ""),
                        "raw1": len(debug.get("raw1") or ""),
                        "raw2": len(debug.get("raw2") or ""),
                    }
                },
            }
        ),
        200,
    )


def create_dw_blueprint(**_kwargs):
    return dw_bp
