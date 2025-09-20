from __future__ import annotations

import csv
import json
import logging
import os
import pathlib
from datetime import date, datetime, timedelta
from typing import Any, Dict, Optional

from flask import Blueprint, current_app, jsonify, request
from sqlalchemy import text

from core.datasources import DatasourceRegistry
from core.settings import Settings
from core.sql_exec import get_mem_engine
from .llm import nl_to_sql_with_llm
from .validator import validate_sql


ALLOWED_COLUMNS = [
    "CONTRACT_ID",
    "CONTRACT_OWNER",
    "CONTRACT_STAKEHOLDER_1",
    "CONTRACT_STAKEHOLDER_2",
    "CONTRACT_STAKEHOLDER_3",
    "CONTRACT_STAKEHOLDER_4",
    "CONTRACT_STAKEHOLDER_5",
    "CONTRACT_STAKEHOLDER_6",
    "CONTRACT_STAKEHOLDER_7",
    "CONTRACT_STAKEHOLDER_8",
    "DEPARTMENT_1",
    "DEPARTMENT_2",
    "DEPARTMENT_3",
    "DEPARTMENT_4",
    "DEPARTMENT_5",
    "DEPARTMENT_6",
    "DEPARTMENT_7",
    "DEPARTMENT_8",
    "OWNER_DEPARTMENT",
    "CONTRACT_VALUE_NET_OF_VAT",
    "VAT",
    "CONTRACT_PURPOSE",
    "CONTRACT_SUBJECT",
    "START_DATE",
    "END_DATE",
    "REQUEST_DATE",
    "REQUEST_TYPE",
    "CONTRACT_STATUS",
    "ENTITY_NO",
    "REQUESTER",
]

ALLOWED_BINDS = [
    "contract_id_pattern",
    "date_end",
    "date_start",
    "dept",
    "entity_no",
    "owner_name",
    "request_type",
    "top_n",
]

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


def _ensure_file_logger(app):
    """Attach a daily log file handler writing to logs/dw-YYYYMMDD.log."""

    try:
        log_dir = os.environ.get("DW_LOG_DIR", "logs")
        os.makedirs(log_dir, exist_ok=True)
        today = datetime.now().strftime("%Y%m%d")
        log_path = os.path.join(log_dir, f"dw-{today}.log")

        for handler in app.logger.handlers:
            if isinstance(handler, logging.FileHandler) and getattr(handler, "_dw_path", "") == log_path:
                return

        fileh = logging.FileHandler(log_path, encoding="utf-8")
        fileh.setLevel(logging.DEBUG if DW_DEBUG else logging.INFO)
        fileh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s"))
        fileh._dw_path = log_path  # type: ignore[attr-defined]

        app.logger.addHandler(fileh)
        app.logger.setLevel(logging.DEBUG if DW_DEBUG else logging.INFO)
        app.logger.info("[dw] logging initialized")
    except Exception as exc:  # pragma: no cover
        print(f"Failed to init dw logger: {exc}")


def _log(tag, payload):
    """Structured logging helper scoped to DW blueprint."""

    try:
        current_app.logger.info(
            f"[dw] {tag}: {json.dumps(payload, default=str)[:4000]}"
        )
    except Exception:
        print(f"[dw] {tag}: {payload}")


def _parse_iso_datetime(value) -> Optional[datetime]:
    if value is None:
        return None
    try:
        if isinstance(value, datetime):
            return value
        if isinstance(value, date):
            return datetime.combine(value, datetime.min.time())
        parsed = datetime.fromisoformat(str(value))
        return parsed
    except Exception:
        return None


def _derive_dates_from_intent(intent: Dict[str, Any]) -> tuple[datetime, datetime]:
    now = datetime.utcnow()
    default_end = now
    default_start = now - timedelta(days=30)

    if not isinstance(intent, dict):
        return default_start, default_end

    explicit = intent.get("explicit_dates")
    if isinstance(explicit, dict):
        start_val = _parse_iso_datetime(explicit.get("start"))
        end_val = _parse_iso_datetime(explicit.get("end"))
        start = start_val or default_start
        end = end_val or default_end
        if start > end:
            start, end = end, start
        return start, end

    if intent.get("has_time_window"):
        return default_start, default_end

    return default_start, default_end


@dw_bp.record_once
def _init_dw_logging(setup_state):
    _ensure_file_logger(setup_state.app)


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
    settings = _settings()
    ds_registry = DatasourceRegistry(settings=settings, namespace=NAMESPACE)
    mem = get_mem_engine(settings)

    body = request.get_json(force=True, silent=False) or {}
    q = (body.get("question") or "").strip()
    auth_email = body.get("auth_email")
    prefixes = body.get("prefixes") or []
    include_debug = DW_INCLUDE_DEBUG or (request.args.get("debug") == "true")

    table_name = settings.get("DW_CONTRACT_TABLE", scope="namespace") or "Contract"
    default_date_col = settings.get("DW_DATE_COLUMN", scope="namespace") or "REQUEST_DATE"

    llm_context = {
        "table": table_name,
        "allowed_columns": ALLOWED_COLUMNS,
        "allowed_binds": ALLOWED_BINDS,
        "default_date_col": default_date_col,
        "allowed_columns_clause": ", ".join(ALLOWED_COLUMNS),
        "binds_whitelist": ", ".join(ALLOWED_BINDS),
        "unpivot_hint": "SELECT CONTRACT_ID, NVL(CONTRACT_VALUE_NET_OF_VAT,0)+NVL(VAT,0) AS CONTRACT_VALUE_GROSS, CONTRACT_STAKEHOLDER_i AS STAKEHOLDER, REQUEST_DATE AS REF_DATE (UNION ALL slots 1..8)",
    }

    with mem.begin() as conn:
        inq_id = conn.execute(
            text(
                """
            INSERT INTO mem_inquiries(namespace, question, auth_email, prefixes, status, created_at, updated_at)
            VALUES (:ns, :q, :email, CAST(:pfx AS jsonb), 'open', NOW(), NOW())
            RETURNING id
        """
            ),
            {"ns": NAMESPACE, "q": q, "email": auth_email, "pfx": json.dumps(prefixes)},
        ).scalar_one()

    _log("inquiry_start", {"id": inq_id, "q": q, "email": auth_email})

    llm_out = nl_to_sql_with_llm(q, llm_context)
    debug_blob = llm_out.get("debug", {}) if isinstance(llm_out, dict) else {}
    clarifier_dbg = debug_blob.get("clarifier") or {}
    intent = clarifier_dbg.get("intent") or {}
    if intent:
        _log("clarifier_intent", intent)
    clar_raw = clarifier_dbg.get("raw")
    if clar_raw:
        payload = {"size": len(clar_raw)}
        if include_debug:
            payload["text"] = clar_raw[:900]
        _log("clarifier_raw", payload)

    prompt_label = debug_blob.get("prompt", "")
    _log("sql_prompt", {"prompt": prompt_label if include_debug else "<hidden>"})
    raw1 = debug_blob.get("raw1", "") or ""
    _log("llm_raw_pass1", {"size": len(raw1)})
    raw2 = debug_blob.get("raw2")
    if raw2:
        _log("llm_raw_pass2", {"size": len(raw2)})

    sql_final = (llm_out.get("sql") or "").strip()
    sql_payload = {"size": len(sql_final)}
    sql_payload["sql"] = sql_final[:900] if include_debug else "<hidden>"
    _log("final_sql", sql_payload)

    validation = validate_sql(sql_final)
    _log("validation", validation)

    if not llm_out.get("ok"):
        with mem.begin() as conn:
            conn.execute(
                text(
                    """
                UPDATE mem_inquiries
                   SET status = 'needs_clarification',
                       last_sql = :sql,
                       last_error = :err,
                       updated_at = NOW()
                 WHERE id = :id
            """
                ),
                {
                    "sql": sql_final,
                    "err": llm_out.get("error") or "validation_failed",
                    "id": inq_id,
                },
            )
        response = {
            "ok": False,
            "status": "needs_clarification",
            "inquiry_id": inq_id,
            "error": llm_out.get("error") or "validation_failed",
            "sql": sql_final,
            "questions": [
                "I couldn’t derive a clean SELECT. Can you rephrase or specify filters (stakeholders, departments, date columns)?",
            ],
        }
        if include_debug:
            response["debug"] = debug_blob
        return jsonify(response)

    if not validation.get("ok"):
        with mem.begin() as conn:
            conn.execute(
                text(
                    """
                UPDATE mem_inquiries
                   SET status = 'needs_clarification',
                       last_sql = :sql,
                       last_error = :err,
                       updated_at = NOW()
                 WHERE id = :id
            """
                ),
                {
                    "sql": sql_final,
                    "err": ",".join(validation.get("errors") or []),
                    "id": inq_id,
                },
            )
        res = {
            "ok": False,
            "status": "needs_clarification",
            "inquiry_id": inq_id,
            "error": (validation.get("errors") or ["invalid_sql"])[0],
            "sql": sql_final,
            "questions": [
                "I couldn’t derive a clean SELECT. Can you rephrase or specify filters (stakeholders, departments, date columns)?",
            ],
        }
        if include_debug:
            res["debug"] = debug_blob
        return jsonify(res)

    bind_names = validation.get("bind_names") or []
    exec_binds: Dict[str, Any] = {}
    if "date_start" in bind_names or "date_end" in bind_names:
        start_dt, end_dt = _derive_dates_from_intent(intent)
        if "date_start" in bind_names:
            exec_binds["date_start"] = start_dt
        if "date_end" in bind_names:
            exec_binds["date_end"] = end_dt
    if "top_n" in bind_names and intent.get("top_n") is not None:
        try:
            exec_binds["top_n"] = int(intent["top_n"])
        except Exception:
            exec_binds["top_n"] = intent["top_n"]

    missing_binds = [name for name in bind_names if name not in exec_binds]
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
        resp = {
            "ok": False,
            "status": "needs_clarification",
            "inquiry_id": inq_id,
            "error": "missing_bind_values",
            "sql": sql_final,
            "questions": [
                f"Provide values for: {', '.join(sorted(missing_binds))} or rephrase with explicit filters."
            ],
        }
        if include_debug:
            resp["debug"] = debug_blob
        return jsonify(resp)
    _log("execution_binds", {k: str(v) for k, v in exec_binds.items()})

    oracle_engine = ds_registry.engine(None)
    rows = []
    headers = []
    error = None
    started = datetime.utcnow()
    try:
        with oracle_engine.begin() as oc:
            rs = oc.execute(text(sql_final), exec_binds)
            headers = list(rs.keys())
            rows = rs.fetchall()
    except Exception as exc:
        error = str(exc)
        _log("oracle_error", {"error": error})

    duration_ms = int((datetime.utcnow() - started).total_seconds() * 1000)
    _log("execution_result", {"rows": len(rows), "cols": headers, "ms": duration_ms})

    if error:
        with mem.begin() as conn:
            conn.execute(
                text(
                    """
                UPDATE mem_inquiries
                   SET status = 'failed',
                       last_sql = :sql,
                       last_error = :err,
                       updated_at = NOW()
                 WHERE id = :id
            """
                ),
                {"sql": sql_final, "err": error, "id": inq_id},
            )
        return jsonify({"ok": False, "error": error, "inquiry_id": inq_id, "status": "failed"})

    csv_path = None
    if rows:
        csv_path = _write_csv(rows, headers)
        if csv_path:
            _log("csv_export", {"path": str(csv_path)})

    autosave = bool(settings.get("SNIPPETS_AUTOSAVE", scope="namespace", default=True))
    snippet_id = None
    if autosave and rows:
        with mem.begin() as conn:
            snippet_id = conn.execute(
                text(
                    """
                INSERT INTO mem_snippets(namespace, title, description, sql_template, sql_raw,
                                         input_tables, output_columns, tags, is_verified, created_at, updated_at)
                VALUES (:ns, :title, :desc, :tmpl, :raw, CAST(:inputs AS jsonb), CAST(:cols AS jsonb),
                        CAST(:tags AS jsonb), :verified, NOW(), NOW())
                RETURNING id
            """
                ),
                {
                    "ns": NAMESPACE,
                    "title": f"dw auto: {q[:60]}",
                    "desc": "Auto-saved by DW pipeline",
                    "tmpl": sql_final,
                    "raw": sql_final,
                    "inputs": json.dumps([table_name]),
                    "cols": json.dumps(headers),
                    "tags": json.dumps(["dw", "contracts", "auto"]),
                    "verified": False,
                },
            ).scalar_one()
        _log("snippet_saved", {"id": snippet_id})

    with mem.begin() as conn:
        conn.execute(
            text(
                """
            UPDATE mem_inquiries
               SET status='answered', answered_by=:by, answered_at=NOW(), updated_at=NOW(),
                   last_sql=:sql, last_error=NULL
             WHERE id=:id
        """
            ),
            {"by": auth_email, "sql": sql_final, "id": inq_id},
        )

    binds_public = {
        name: (value.isoformat() if hasattr(value, "isoformat") else value)
        for name, value in exec_binds.items()
    }
    used_repair = bool(debug_blob.get("sql2"))
    meta = {
        "rowcount": len(rows),
        "columns": headers,
        "duration_ms": duration_ms,
        "used_repair": used_repair,
        "suggested_date_column": intent.get("date_column") or default_date_col,
        "clarifier_intent": intent,
        "binds": binds_public,
    }

    resp = {
        "ok": True,
        "inquiry_id": inq_id,
        "sql": sql_final,
        "rows": [list(r) for r in rows[:200]],
        "csv_path": str(csv_path) if csv_path else None,
        "meta": meta,
    }
    if include_debug:
        resp["debug"] = debug_blob
    return jsonify(resp)




def create_dw_blueprint(*args, **kwargs):
    """Factory function returning the DocuWare blueprint."""
    return dw_bp
