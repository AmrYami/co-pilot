import os, json, logging
from datetime import datetime, timedelta
from flask import Blueprint, request, jsonify, current_app
from logging.handlers import TimedRotatingFileHandler

from .llm import nl_to_sql_with_llm
from .validator import validate_sql, find_named_binds
from core.sql_exec import get_oracle_engine, get_mem_engine

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
    final_sql = (llm_out.get("final_sql") or "").strip()
    allow_binds = {"date_start","date_end","top_n","owner_name","dept","entity_no","contract_id_pattern","request_type"}
    val = validate_sql(final_sql, allow_binds)

    _log("final_sql", {
        "pass": llm_out.get("pass"),
        "ok": val["ok"],
        "errors": val["errors"],
        "binds": val["binds"],
        "sql_preview": final_sql[:4000]
    })

    if not val["ok"] or not final_sql:
        return jsonify({
            "ok": False,
            "status": "needs_clarification",
            "error": (val["errors"][0] if val["errors"] else "empty_sql"),
            "sql": final_sql or None,
            "questions": ["I couldn't derive a clean SELECT. Please rephrase or specify the filters/window clearly."],
            "debug": {
                "prompt": llm_out.get("prompt"),
                "raw1_len": len(llm_out.get("raw1") or ""),
                "raw2_len": len(llm_out.get("raw2") or ""),
                "sql1": llm_out.get("sql1"),
                "sql2": llm_out.get("sql2"),
                "val1": llm_out.get("val1"),
                "val2": llm_out.get("val2"),
            }
        }), 200

    # 5) Resolve binds (you can expand this as needed)
    bind_names = set(val["binds"])
    binds = {}

    # Heuristic windows for demo; you can extend with clarifier intent
    if "date_start" in bind_names or "date_end" in bind_names:
        ds, de = last_month_window()  # because your example asks "last month"
        binds["date_start"] = ds
        binds["date_end"] = de

    if "top_n" in bind_names:
        # many queries hard-code FETCH FIRST N, but if model used :top_n we populate 10
        binds["top_n"] = 10

    # Just log binds chosen for execution
    _log("execution_binds", {k: (str(v) if hasattr(v, "isoformat") else v) for k, v in binds.items()})

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
            "sample": rows[:3]
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
