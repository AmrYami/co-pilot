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


def _insert_inquiry(conn, namespace: str, question: str, auth_email: str, prefixes):
    """Insert a mem inquiry using JSONB-aware binds."""

    stmt = text(
        """
        INSERT INTO mem_inquiries(namespace, question, auth_email, prefixes, status, created_at, updated_at)
        VALUES (:ns, :q, :auth, CAST(:pfx AS jsonb), 'open', NOW(), NOW())
        RETURNING id
        """
    )
    params = {
        "ns": namespace,
        "q": question,
        "auth": auth_email,
        "pfx": json.dumps(prefixes or []),
    }
    return conn.execute(stmt, params).scalar_one()


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
        inq_id = _insert_inquiry(conn, namespace, question, auth_email, prefixes)

    _log("inquiry_start", {"id": inq_id, "q": question, "email": auth_email})

    llm_context = {"namespace": namespace, "table": "Contract", "log": _log}

    out = nl_to_sql_with_llm(question, llm_context)
    intent = out.get("intent") or {}
    final_sql = out.get("final_sql") or ""
    final_pass = f"pass{out.get('pass')}" if out.get("pass") else "unknown"
    trunc_sql = (final_sql[:600] + "...") if len(final_sql) > 600 else final_sql
    current_app.logger.info(
        "[dw] chosen_sql",
        extra={"payload": {"pass": final_pass, "sql": trunc_sql}},
    )

    if os.getenv("DW_DEBUG", "0") == "1":
        _log(
            "llm_pass_debug",
            {
                "raw1": (out.get("raw1") or "")[:240],
                "sql1": (out.get("sql1") or "")[:240],
                "raw2": (out.get("raw2") or "")[:240],
                "sql2": (out.get("sql2") or "")[:240],
            },
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

    exec_binds = {k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in binds.items()}
    _log("sql_final", {"sql": final_sql})
    _log("sql_binds", exec_binds)
    _log("choose_sql", {"pass": out.get("pass"), "preview": (final_sql or "")[:240], "binds": exec_binds})

    if os.getenv("DW_INCLUDE_DEBUG", "0") == "1":
        current_app.logger.info("[dw] will_execute: sql_preview=%s", (final_sql[:300] if final_sql else None))
        current_app.logger.info("[dw] will_execute: binds=%s", binds)

    if not final_sql or not final_sql.strip().lower().startswith(("select", "with")):
        llm_debug = {
            "pass": out.get("pass"),
            "ok": out.get("ok"),
            "errors": out.get("errors"),
            "sql1": out.get("sql1"),
            "sql2": out.get("sql2"),
            "raw1": out.get("raw1"),
            "raw2": out.get("raw2"),
        }
        return jsonify({
            "ok": False,
            "status": "needs_clarification",
            "error": "empty_sql",
            "questions": ["I couldn't derive a clean SELECT. Can you rephrase or specify filters (stakeholders, departments, date columns)?"],
            "debug": {"llm": llm_debug, "clarifier": out.get("clarifier")},
        })

    try:
        preview_sql = final_sql[:600] + (" ..." if len(final_sql) > 600 else "")
        current_app.logger.info("[dw] exec_sql_preview: %s", preview_sql)
        safe_binds = {
            key: (value if isinstance(value, (str, int, float)) else str(value))
            for key, value in (binds or {}).items()
        }
        current_app.logger.info("[dw] exec_binds: %s", json.dumps(safe_binds, default=str))
        current_app.logger.info("[dw] final_sql: %s", final_sql)
        current_app.logger.info("[dw] execution_binds: %s", safe_binds)
    except Exception:
        pass

    engine = get_oracle_engine()
    rows: list[list[object]] = []
    cols: list[str] = []
    started = datetime.utcnow()
    try:
        _log("execution_sql", {"sql": final_sql[:1800]})
        _log("execution_binds", exec_binds)
        with engine.begin() as conn:
            result = conn.exec_driver_sql(final_sql, binds)
            cols = list(result.keys())
            rows = [list(row) for row in result.fetchall()]
        elapsed_ms = int((datetime.utcnow() - started).total_seconds() * 1000)
        _log("execution_result", {"rows": len(rows), "cols": cols, "ms": elapsed_ms})
    except Exception as exc:  # pragma: no cover - surface database errors
        _log("oracle_error", {"error": str(exc)})
        llm_debug = {
            "pass": out.get("pass"),
            "ok": out.get("ok"),
            "errors": out.get("errors"),
            "sql1": out.get("sql1"),
            "sql2": out.get("sql2"),
            "raw1": out.get("raw1"),
            "raw2": out.get("raw2"),
        }
        return (jsonify({
            "ok": False,
            "status": "failed",
            "error": str(exc),
            "sql": final_sql,
            "binds": binds,
            "debug": {"llm": llm_debug, "clarifier": out.get("clarifier")},
        }), 200)

    return (
        jsonify(
            {
                "ok": True,
                "rows": rows,
                "columns": cols,
                "sql": final_sql,
                "binds": {k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in binds.items()},
                "intent": intent,
                "debug": {
                    "llm": {
                        "pass": out.get("pass"),
                        "ok": out.get("ok"),
                        "errors": out.get("errors"),
                        "sql1": out.get("sql1"),
                        "sql2": out.get("sql2"),
                        "raw1": out.get("raw1"),
                        "raw2": out.get("raw2"),
                    },
                    "clarifier": out.get("clarifier"),
                },
            }
        ),
        200,
    )


def create_dw_blueprint(**_kwargs):
    return dw_bp
