from __future__ import annotations

import csv
import json
import logging
import os
import pathlib
import re
from datetime import date, datetime, timedelta
from logging.handlers import TimedRotatingFileHandler

from flask import Blueprint, jsonify, current_app, request
from sqlalchemy import text
from core.settings import Settings
from core.datasources import DatasourceRegistry
from core.sql_exec import get_mem_engine
from .llm import ALLOWED_BINDS, nl_to_sql_with_llm, repair_sql
from .validator import validate_sql


dw_bp = Blueprint("dw", __name__, url_prefix="/dw")


def _env_truthy(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() not in {"", "0", "false", "no"}


NAMESPACE = os.environ.get("DW_NAMESPACE", "dw::common")
DW_DEBUG = _env_truthy("DW_DEBUG")
DW_INCLUDE_DEBUG = _env_truthy("DW_INCLUDE_DEBUG", default=True)


def _settings():
    return Settings()


def _ensure_log_dir_and_handler(app):
    """Create logs/ and attach a daily rotating file handler once."""

    try:
        log_dir = os.environ.get("DW_LOG_DIR", "logs")
        os.makedirs(log_dir, exist_ok=True)
        log_path = os.path.join(log_dir, "dw.log")

        existing = [h for h in app.logger.handlers if getattr(h, "_dw_handler", False)]
        if existing:
            return

        debug_level = logging.DEBUG if DW_DEBUG else logging.INFO

        console = logging.StreamHandler()
        console.setLevel(debug_level)
        console.setFormatter(logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s"))

        fileh = TimedRotatingFileHandler(log_path, when="midnight", backupCount=14, encoding="utf-8")
        fileh.setLevel(debug_level)
        fileh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s"))
        fileh._dw_handler = True  # type: ignore[attr-defined]

        app.logger.setLevel(debug_level)
        app.logger.addHandler(console)
        app.logger.addHandler(fileh)
        app.logger.info("[dw] logging initialized")
    except Exception as exc:  # pragma: no cover - defensive logging setup
        print(f"[dw] log init failed: {exc}")


def _log(tag, payload):
    """Structured logging helper scoped to DW blueprint."""

    try:
        current_app.logger.info(
            f"[dw] {tag}: {json.dumps(payload, default=str)[:4000]}"
        )
    except Exception:
        print(f"[dw] {tag}: {payload}")


@dw_bp.record_once
def _init_dw_logging(setup_state):
    app = setup_state.app
    _ensure_log_dir_and_handler(app)


def _question_has_window(q: str) -> bool:
    ql = (q or "").lower()
    return any(k in ql for k in [
        "last month", "next ", "last ", "between ", "in ", "since ", "days", "months", "year", "quarter"
    ])

def _derive_dates_for_question(q: str) -> dict:
    """Compute :date_start/:date_end for common phrases. Return {} if not applicable."""
    ql = (q or "").lower()
    today = date.today()
    if "next 30 days" in ql:
        return {"date_start": datetime.combine(today, datetime.min.time()),
                "date_end":   datetime.combine(today + timedelta(days=30), datetime.min.time())}
    if "last month" in ql:
        first_of_this = date(today.year, today.month, 1)
        last_month_end = first_of_this - timedelta(days=1)
        last_month_start = date(last_month_end.year, last_month_end.month, 1)
        return {
            "date_start": datetime.combine(last_month_start, datetime.min.time()),
            "date_end":   datetime.combine(first_of_this, datetime.min.time()),
        }
    m = re.search(r"last\s+(\d+)\s+days", ql)
    if m:
        n = int(m.group(1))
        return {"date_start": datetime.combine(today - timedelta(days=n), datetime.min.time()),
                "date_end":   datetime.combine(today, datetime.min.time())}
    m = re.search(r"in\s+(20\d{2})", ql)
    if m:
        yr = int(m.group(1))
        return {"date_start": datetime(yr,1,1), "date_end": datetime(yr+1,1,1)}
    m = re.search(r"between\s+(\d{4}-\d{2}-\d{2})\s+and\s+(\d{4}-\d{2}-\d{2})", ql)
    if m:
        a,b = m.group(1), m.group(2)
        return {"date_start": datetime.fromisoformat(a), "date_end": datetime.fromisoformat(b)}
    return {}


def _write_csv(rows, headers) -> str:
    if not rows:
        return None
    out_dir = pathlib.Path(os.environ.get("DW_EXPORT_DIR", "/tmp/dw_exports"))
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    path = out_dir / f"dw_{ts}.csv"
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(headers)
        for row in rows:
            writer.writerow(row)
    return path


@dw_bp.route("/answer", methods=["POST"])
def answer():
    s = _settings()
    ds_registry = DatasourceRegistry(settings=s, namespace=NAMESPACE)
    mem = get_mem_engine(s)
    body = request.get_json(force=True, silent=False) or {}
    q = body.get("question","").strip()
    auth_email = body.get("auth_email")
    prefixes = body.get("prefixes") or []
    include_debug = DW_INCLUDE_DEBUG or (request.args.get("debug") == "true")

    table_name = s.get("DW_CONTRACT_TABLE", scope="namespace") or "Contract"
    default_date_col = s.get("DW_DATE_COLUMN", scope="namespace") or "REQUEST_DATE"
    bind_whitelist = set(ALLOWED_BINDS)

    # Create inquiry row (status open)
    with mem.begin() as conn:
        inq_id = conn.execute(text("""
            INSERT INTO mem_inquiries(namespace, question, auth_email, prefixes, status, created_at, updated_at)
            VALUES (:ns, :q, :email, CAST(:pfx AS jsonb), 'open', NOW(), NOW())
            RETURNING id
        """), {"ns": NAMESPACE, "q": q, "email": auth_email, "pfx": json.dumps(prefixes)}).scalar_one()

    _log("inquiry_start", {"id": inq_id, "q": q, "email": auth_email})

    # ---------- LLM first pass ----------
    llm_context = {
        "contract_table": table_name,
        "default_date_col": default_date_col,
    }
    llm_out = nl_to_sql_with_llm(q, llm_context)
    intent_bundle = llm_out.get("intent") or {}
    _log("clarifier_intent", intent_bundle)
    prompt_text = llm_out.get("prompt") or ""
    _log("sql_prompt", {"prompt": prompt_text[:900]})
    raw1 = llm_out.get("raw1") or ""
    _log("llm_raw_pass1", {"text": raw1[:900]})
    sql1 = (llm_out.get("sql1") or "").strip()
    _log("llm_sql_pass1", {"sql": sql1[:900]})
    intent = intent_bundle.get("intent", {}) if isinstance(intent_bundle, dict) else {}

    # ---------- Validate ----------
    v1 = validate_sql(sql1, allow_tables=[table_name], bind_whitelist=bind_whitelist)
    _log("validation_pass1", v1)

    # ---------- Repair pass if needed ----------
    sql_final = sql1
    v_final = v1
    used_repair = False
    if not v1["ok"]:
        repair_out = repair_sql(sql1, prompt_text, q)
        raw2 = repair_out.get("raw2") or ""
        sql2 = (repair_out.get("sql2") or "").strip()
        _log("llm_raw_pass2", {"text": raw2[:900]})
        _log("llm_sql_pass2", {"sql": sql2[:900]})
        v2 = validate_sql(sql2, allow_tables=[table_name], bind_whitelist=bind_whitelist)
        _log("validation_pass2", v2)
        if v2["ok"]:
            sql_final = sql2
            v_final = v2
            used_repair = True

    # ---------- If still not ok → needs clarification ----------
    if not v_final["ok"]:
        with mem.begin() as conn:
            conn.execute(text("""
                UPDATE mem_inquiries
                   SET status = 'needs_clarification',
                       last_sql = :sql,
                       last_error = :err,
                       updated_at = NOW()
                 WHERE id = :id
            """), {"sql": sql_final, "err": ",".join(v_final["errors"]), "id": inq_id})
        res = {
            "ok": False,
            "status": "needs_clarification",
            "inquiry_id": inq_id,
            "error": v_final["errors"][0] if v_final["errors"] else "error",
            "sql": sql_final,
            "questions": [
                "I couldn't derive a clean SELECT. Can you rephrase or specify filters (stakeholders, departments, date columns)?"
            ],
        }
        if include_debug:
            res["debug"] = {
                "clarifier": intent_bundle,
                "prompt": prompt_text,
                "raw1": raw1,
                "sql1": sql1,
                "validation1": v1,
                "used_repair": used_repair,
            }
        return jsonify(res)

    # ---------- Prepare binds ----------
    binds_used = set(v_final["binds"])
    binds = {}
    phrase_window_hint = _question_has_window(q)
    explicit_dates = intent.get("explicit_dates") if isinstance(intent.get("explicit_dates"), dict) else None
    needs_window = {"date_start", "date_end"} & binds_used
    wants_window = bool(intent.get("has_time_window")) or phrase_window_hint
    question_has_window = wants_window

    if needs_window or wants_window:
        candidate_window = {}
        if explicit_dates and explicit_dates.get("start") and explicit_dates.get("end"):
            try:
                candidate_window = {
                    "date_start": datetime.fromisoformat(explicit_dates["start"]),
                    "date_end": datetime.fromisoformat(explicit_dates["end"]),
                }
            except Exception:
                candidate_window = {}
        if not candidate_window:
            candidate_window = _derive_dates_for_question(q) if wants_window else {}
        if not candidate_window and needs_window:
            today = date.today()
            candidate_window = {
                "date_start": datetime.combine(today - timedelta(days=30), datetime.min.time()),
                "date_end": datetime.combine(today, datetime.min.time()),
            }
        if candidate_window.get("date_start") and candidate_window.get("date_end"):
            binds["date_start"] = candidate_window["date_start"]
            binds["date_end"] = candidate_window["date_end"]
        elif needs_window:
            with mem.begin() as conn:
                conn.execute(
                    text(
                        """
                    UPDATE mem_inquiries
                       SET status='needs_clarification', last_sql=:sql, last_error='missing_window_values', updated_at=NOW()
                     WHERE id=:id
                """
                    ),
                    {"sql": sql_final, "id": inq_id},
                )
            res = {
                "ok": False,
                "status": "needs_clarification",
                "inquiry_id": inq_id,
                "error": "missing_window_values",
                "sql": sql_final,
                "questions": [
                    "I couldn't determine the time window values. Please specify start and end dates explicitly."
                ],
            }
            if include_debug:
                res["debug"] = {
                    "clarifier": intent_bundle,
                    "prompt": prompt_text,
                    "raw1": raw1,
                    "sql1": sql1,
                    "validation1": v1,
                    "used_repair": used_repair,
                }
            return jsonify(res)

    if "top_n" in binds_used:
        top_n_val = intent.get("top_n")
        if not isinstance(top_n_val, int):
            m_top = re.search(r"top\s+(\d+)", q.lower())
            if m_top:
                try:
                    top_n_val = int(m_top.group(1))
                except Exception:
                    top_n_val = None
        binds["top_n"] = top_n_val or 10

    missing_binds = [b for b in binds_used if b not in binds]
    if missing_binds:
        with mem.begin() as conn:
            conn.execute(
                text(
                    """
                UPDATE mem_inquiries
                   SET status='needs_clarification', last_sql=:sql, last_error='missing_bind_values', updated_at=NOW()
             WHERE id=:id
            """
                ),
                {"sql": sql_final, "id": inq_id},
            )
        res = {
            "ok": False,
            "status": "needs_clarification",
            "inquiry_id": inq_id,
            "error": "missing_bind_values",
            "sql": sql_final,
            "questions": [
                f"Provide values for: {', '.join(missing_binds)} or rephrase with explicit filters."
            ],
        }
        if include_debug:
            res["debug"] = {
                "clarifier": intent_bundle,
                "prompt": prompt_text,
                "raw1": raw1,
                "sql1": sql1,
                "validation1": v1,
                "used_repair": used_repair,
            }
        return jsonify(res)

    _log("execution_binds", {k: str(v) for k, v in binds.items()})

    # ---------- Execute on Oracle ----------
    oracle_engine = ds_registry.engine(None)
    rows = []
    headers = []
    error = None
    started = datetime.utcnow()
    try:
        with oracle_engine.begin() as oc:
            rs = oc.execute(text(sql_final), binds)
            headers = list(rs.keys())
            rows = rs.fetchall()
    except Exception as ex:
        error = str(ex)
        _log(
            "oracle_error",
            {
                "error": error,
                "sql": sql_final[:4000],
                "binds": {k: str(v) for k, v in binds.items()},
            },
        )

    duration_ms = int((datetime.utcnow() - started).total_seconds() * 1000)
    _log("execution_result", {"rows": len(rows), "cols": headers, "ms": duration_ms})

    if error:
        with mem.begin() as conn:
            conn.execute(text("""
                UPDATE mem_inquiries
                   SET status = 'failed',
                       last_sql = :sql,
                       last_error = :err,
                       updated_at = NOW()
                 WHERE id = :id
            """), {"sql": sql_final, "err": error, "id": inq_id})
        return jsonify({"ok": False, "error": error, "inquiry_id": inq_id, "status": "failed"})

    # ---------- CSV export ----------
    csv_path = None
    if rows:
        csv_path = _write_csv(rows, headers)
        _log("csv_export", {"path": csv_path})

    # ---------- Auto-save snippet (if enabled) ----------
    autosave = bool(s.get("SNIPPETS_AUTOSAVE", scope="namespace", default=True))
    snippet_id = None
    if autosave and rows:
        with mem.begin() as conn:
            snippet_id = conn.execute(text("""
                INSERT INTO mem_snippets(namespace, title, description, sql_template, sql_raw,
                                         input_tables, output_columns, tags, is_verified, created_at, updated_at)
                VALUES (:ns, :title, :desc, :tmpl, :raw, CAST(:inputs AS jsonb), CAST(:cols AS jsonb),
                        CAST(:tags AS jsonb), :verified, NOW(), NOW())
                RETURNING id
            """), {
                "ns": NAMESPACE,
                "title": f"dw auto: {q[:60]}",
                "desc": "Auto-saved by DW pipeline",
                "tmpl": sql_final,
                "raw": sql_final,
                "inputs": json.dumps([table_name]),
                "cols": json.dumps(headers),
                "tags": json.dumps(["dw","contracts","auto"]),
                "verified": False,
            }).scalar_one()
        _log("snippet_saved", {"id": snippet_id})

    # ---------- Mark inquiry answered ----------
    with mem.begin() as conn:
        conn.execute(text("""
            UPDATE mem_inquiries
               SET status='answered', answered_by=:by, answered_at=NOW(), updated_at=NOW(),
                   last_sql=:sql, last_error=NULL
             WHERE id=:id
        """), {"by": auth_email, "sql": sql_final, "id": inq_id})

    # ---------- Response ----------
    meta = {
        "rowcount": len(rows),
        "columns": headers,
        "duration_ms": duration_ms,
        "used_repair": used_repair,
        "question_has_window": question_has_window,
        "suggested_date_column": intent.get("date_column") or default_date_col,
        "clarifier": intent_bundle,
        "binds": {k: str(v) for k, v in binds.items()},
    }
    csv_path_str = str(csv_path) if csv_path else None
    resp = {
        "ok": True,
        "inquiry_id": inq_id,
        "sql": sql_final,
        "rows": [list(r) for r in rows[:200]],  # cap preview
        "csv_path": csv_path_str,
        "meta": meta,
    }
    if include_debug:
        resp["debug"] = {
            "clarifier": intent_bundle,
            "prompt": prompt_text,
            "raw1": raw1,
            "sql1": sql1,
            "validation1": v1,
            "used_repair": used_repair,
        }
    return jsonify(resp)


def create_dw_blueprint(*args, **kwargs):
    """Factory function returning the DocuWare blueprint."""
    return dw_bp
