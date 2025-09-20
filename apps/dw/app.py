import json
import logging
import os
from datetime import datetime
from flask import Blueprint, request, jsonify, current_app
from logging.handlers import TimedRotatingFileHandler

from .llm import nl_to_sql_with_llm
from .validator import analyze_sql
from core.sql_exec import get_oracle_engine

dw_bp = Blueprint("dw", __name__, url_prefix="/dw")

# ---------- logging helpers ----------
def _ensure_file_logging():
    app = current_app
    if app.config.get("DW_LOGGING_READY"):
        return
    os.makedirs("logs", exist_ok=True)
    log_path = os.path.join("logs", f"dw-{datetime.now().strftime('%Y%m%d')}.log")
    fh = TimedRotatingFileHandler(log_path, when="midnight", backupCount=14, encoding="utf-8")
    fh.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s")
    fh.setFormatter(fmt)
    # Avoid duplicate handlers
    names = [type(h).__name__ for h in app.logger.handlers]
    if "TimedRotatingFileHandler" not in names:
        app.logger.addHandler(fh)
    app.config["DW_LOGGING_READY"] = True
    app.logger.info("[dw] logging initialized")

@dw_bp.before_app_request
def _setup_logging():
    _ensure_file_logging()

def _log(tag: str, payload=None, level="info"):
    msg = f"[dw] {tag}"
    if payload is not None:
        try:
            msg += ": " + json.dumps(payload, default=str)
        except Exception:
            msg += f": {payload}"
    getattr(current_app.logger, level, current_app.logger.info)(msg)

# ---------- tiny date helpers ----------
def last_month_window(today=None):
    d = today or datetime.utcnow()
    first_this = d.replace(day=1)
    last_month_end = first_this
    # previous month start
    if first_this.month == 1:
        start = first_this.replace(year=first_this.year - 1, month=12)
    else:
        start = first_this.replace(month=first_this.month - 1)
    return start, last_month_end

# ---------- main endpoint ----------
@dw_bp.route("/answer", methods=["POST"])
def answer():
    _ensure_file_logging()

    j = request.get_json(force=True, silent=True) or {}
    q = j.get("question", "").strip()
    auth_email = j.get("auth_email")
    prefixes = j.get("prefixes", [])

    # 1) Log inquiry
    # (You already insert to mem_inquiries elsewhere; this is just logging)
    _log("inquiry_start", {"q": q, "email": auth_email})

    # 2) Clarify intent (already done inside LLM context by you; keep context lightweight here)
    #    You may have a dedicated clarifier – or pass minimal hints.
    #    We’ll add the common hint: if question mentions “last month” we compute binds.
    ctx = {}

    # 3) Call two-pass generator (returns all debug)
    llm_out = nl_to_sql_with_llm(q, ctx)

    # 4) Decide final SQL & validate (again)
    final_sql = (llm_out.get("sql") or "").strip()

    _log("llm_outcome", {
        "used_repair": llm_out.get("used_repair"),
        "intent": llm_out.get("intent"),
    })

    validation = analyze_sql(final_sql)
    _log("final_sql", {
        "size": len(final_sql),
        "used_repair": llm_out.get("used_repair"),
        "sql_preview": final_sql[:4000],
    })

    llm_binds = llm_out.get("binds") or {}
    _log("binds_detected", {
        "sql_binds": validation.get("binds", []),
        "intent_binds": {k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in llm_binds.items()},
        "validation_errors": validation.get("errors", []),
    })

    if not validation["ok"] or not final_sql:
        first_pass_debug = llm_out.get("first_pass") or {}
        if first_pass_debug:
            first_pass_debug = {
                "sql": first_pass_debug.get("sql"),
                "validation": first_pass_debug.get("validation"),
                "raw_len": len(first_pass_debug.get("raw") or ""),
            }
        return jsonify({
            "ok": False,
            "status": "needs_clarification",
            "error": (validation["errors"][0] if validation["errors"] else "empty_sql"),
            "sql": final_sql or None,
            "questions": ["I couldn't derive a clean SELECT. Please rephrase or specify the filters/window clearly."],
            "debug": {
                "intent": llm_out.get("intent"),
                "raw_len": len(llm_out.get("raw") or ""),
                "validation": validation,
                "first_pass": first_pass_debug,
            }
        }), 200

    # 5) Resolve binds using intent-derived defaults with fallbacks
    bind_names = list(dict.fromkeys(validation.get("binds", [])))
    binds = {}
    for name in bind_names:
        if name in llm_binds and llm_binds[name] is not None:
            binds[name] = llm_binds[name]
            continue
        if name in {"date_start", "date_end"}:
            ds, de = last_month_window()
            if "date_start" in bind_names and "date_start" not in binds:
                binds["date_start"] = ds
            if "date_end" in bind_names and "date_end" not in binds:
                binds["date_end"] = de
        elif name == "top_n":
            binds[name] = 10

    # Just log binds chosen for execution
    _log("execution_binds", {k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in binds.items()})

    # 6) Execute
    rows = []
    cols = []
    elapsed_ms = 0
    try:
        oracle = get_oracle_engine()
        t0 = datetime.utcnow()
        with oracle.begin() as c:
            res = c.exec_driver_sql(final_sql, binds)
            cols = list(res.keys() or [])
            rows = res.fetchall()
        elapsed_ms = int((datetime.utcnow() - t0).total_seconds() * 1000)

        _log("execution_result", {
            "rows": len(rows),
            "cols": cols,
            "ms": elapsed_ms,
            "sample": [list(r) for r in rows[:3]],
        })
    except Exception as e:
        _log("oracle_error", {"error": str(e), "sql": final_sql}, level="error")
        return jsonify({
            "ok": False,
            "status": "failed",
            "error": str(e),
            "sql": final_sql
        }), 200

    return jsonify({
        "ok": True,
        "sql": final_sql,
        "meta": {
            "binds": {k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in binds.items()},
            "rowcount": len(rows),
            "columns": cols,
        },
        "rows": [list(r) for r in rows]
    }), 200
