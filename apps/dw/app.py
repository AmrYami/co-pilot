from __future__ import annotations

import csv
import json
import os
import pathlib
from datetime import date, datetime, timedelta

from flask import Blueprint, jsonify, request
from sqlalchemy import text

from core.datasources import DatasourceRegistry
from core.settings import Settings
from core.model_loader import get_model
from core.sql_exec import get_mem_engine
from core.sql_utils import (
    extract_json_bracket,
    looks_like_instruction,
    sanitize_oracle_sql,
    validate_oracle_sql,
)
from core.logging_utils import get_logger, log_event
from .llm import clarify_intent, derive_bind_values, nl_to_sql_with_llm
from .validator import WHITELIST_BINDS, basic_checks


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

ALLOWED_BINDS = sorted(WHITELIST_BINDS)

dw_bp = Blueprint("dw", __name__, url_prefix="/dw")
log = get_logger("main")


def _env_truthy(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() not in {"", "0", "false", "no"}


NAMESPACE = os.environ.get("DW_NAMESPACE", "dw::common")
DW_INCLUDE_DEBUG = _env_truthy("DW_INCLUDE_DEBUG", default=True)


def _settings():
    return Settings()


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

    ctx = {
        "table": table_name,
        "allowed_columns": ALLOWED_COLUMNS,
        "allowed_binds": ALLOWED_BINDS,
        "default_date_col": default_date_col,
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

    log_event(
        log,
        "dw",
        "inquiry_start",
        {
            "id": inq_id,
            "q": q,
            "email": auth_email,
            "ns": NAMESPACE,
            "prefixes": prefixes,
        },
    )

    clarifier = clarify_intent(q, ctx)
    clarifier_raw = ""
    intent = clarifier.get("intent", {}) if isinstance(clarifier, dict) else {}
    if not isinstance(intent, dict):
        intent = {}
    if isinstance(clarifier, dict):
        clarifier_raw = clarifier.get("raw") or ""

    bracket_payload = extract_json_bracket(clarifier_raw) or {}
    for key in ("has_time_window", "date_column", "top_n", "explicit_dates"):
        value = bracket_payload.get(key)
        if value is not None:
            intent[key] = value

    lowered_question = (q or "").lower()
    if "last month" in lowered_question:
        today = date.today()
        first_this_month = today.replace(day=1)
        last_month_end = first_this_month - timedelta(days=1)
        last_month_start = last_month_end.replace(day=1)
        intent["has_time_window"] = True
        intent["explicit_dates"] = {
            "start": last_month_start.isoformat(),
            "end": last_month_end.isoformat(),
        }
        intent.setdefault("date_column", default_date_col)

    log_event(log, "dw", "clarifier_intent_adjusted", json.loads(json.dumps(intent, default=str)))
    if clarifier_raw and include_debug:
        log_event(
            log,
            "dw",
            "clarifier_raw_debug",
            {"size": len(clarifier_raw), "text": clarifier_raw[:900]},
        )

    llm_out = nl_to_sql_with_llm(q, ctx, intent=intent)
    prompt_text = llm_out.get("prompt") or ""
    raw1 = llm_out.get("raw1") or ""
    raw2 = ""
    if llm_out.get("used_repair"):
        raw2 = llm_out.get("raw2") or ""

    strict_attempted = False
    strict_raw = ""

    def _strict_retry() -> str:
        nonlocal strict_attempted, strict_raw
        if strict_attempted:
            return strict_raw
        strict_attempted = True
        mdl = get_model("sql")
        if mdl is None:
            strict_raw = ""
            log_event(log, "dw", "llm_raw_strict", {"size": 0, "skipped": True})
            return strict_raw
        strict_prompt = (
            "Return only one Oracle SELECT (or CTE) statement.\n"
            "No code fences. No comments. No explanations. No extra text.\n"
            f'Table: "{table_name}". Allowed columns: {", ".join(ALLOWED_COLUMNS)}.\n'
            f"Allowed binds: {', '.join(ALLOWED_BINDS)}.\n"
            f"Use Oracle syntax. Use {intent.get('date_column') or default_date_col} for default window.\n"
            "Question:\n"
            f"{q}\n"
            "Statement:\n"
        )
        try:
            strict_raw = mdl.generate(
                strict_prompt,
                max_new_tokens=200,
                temperature=0.0,
                top_p=0.95,
                stop=["```", "<<JSON>>"],
            )
        except Exception as exc:  # pragma: no cover - logging only
            strict_raw = ""
            log_event(log, "dw", "strict_retry_error", {"error": str(exc)})
        log_event(log, "dw", "llm_raw_strict", {"size": len(strict_raw)})
        return strict_raw

    sql_candidates: list[str] = []
    if raw2:
        sql_candidates.append(raw2)
    if raw1:
        sql_candidates.append(raw1)
    sql_from_llm = llm_out.get("sql") or ""
    if sql_from_llm:
        sql_candidates.append(sql_from_llm)

    sql_final = sanitize_oracle_sql(*sql_candidates)

    def _oracle_parse_error(sql_text: str) -> str | None:
        if not sql_text:
            return "empty_sql_after_sanitize"
        if looks_like_instruction(sql_text):
            return "instruction_echo"
        try:
            validate_oracle_sql(sql_text)
        except ValueError as exc:
            return str(exc)
        return None

    parse_error = _oracle_parse_error(sql_final)
    if parse_error:
        strict_raw = _strict_retry()
        if strict_raw:
            sql_final = sanitize_oracle_sql(strict_raw)
            parse_error = _oracle_parse_error(sql_final)

    sql_payload = {"size": len(sql_final)}
    sql_payload["sql"] = sql_final[:900] if include_debug else "<hidden>"
    log_event(log, "dw", "final_sql", sql_payload)
    validation = llm_out.get("validation") or basic_checks(sql_final, allowed_binds=ALLOWED_BINDS)
    if validation is None or not isinstance(validation, dict):
        validation = basic_checks(sql_final, allowed_binds=ALLOWED_BINDS)
    if parse_error:
        validation = dict(validation)
        validation.setdefault("errors", [])
        validation["errors"].append(f"oracle_parse:{parse_error}")
        validation["ok"] = False
    validation_payload = {
        "ok": bool(validation.get("ok")),
        "errors": validation.get("errors"),
        "binds": validation.get("binds"),
        "bind_names": validation.get("bind_names"),
    }
    log_event(log, "dw", "validation", json.loads(json.dumps(validation_payload, default=str)))

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
        log_event(
            log,
            "dw",
            "inquiry_status",
            {"id": inq_id, "from": "open", "to": "needs_clarification", "reason": "validation_failed"},
        )
        res = {
            "ok": False,
            "status": "needs_clarification",
            "inquiry_id": inq_id,
            "error": (validation.get("errors") or ["error"])[0],
            "sql": sql_final,
            "questions": [
                "I couldn't derive a clean SELECT. Can you rephrase or specify filters (stakeholders, departments, date columns)?"
            ],
        }
        if include_debug:
            debug_payload = {
                "intent": intent,
                "prompt": prompt_text,
                "raw1": raw1,
                "validation": validation,
                "clarifier_raw": clarifier_raw,
            }
            if raw2:
                debug_payload["raw2"] = raw2
            if strict_attempted:
                debug_payload["raw_strict"] = strict_raw
            res["debug"] = debug_payload
        return jsonify(res)

    used_binds = validation.get("binds") or []
    actual_bind_names = validation.get("bind_names") or []
    bind_name_map = {canon: actual for canon, actual in zip(used_binds, actual_bind_names)}

    bind_values = derive_bind_values(q, used_binds, intent)
    missing = [b for b in used_binds if b not in bind_values]
    if missing:
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
        log_event(
            log,
            "dw",
            "inquiry_status",
            {"id": inq_id, "from": "open", "to": "needs_clarification", "reason": "missing_bind_values"},
        )
        resp = {
            "ok": False,
            "status": "needs_clarification",
            "inquiry_id": inq_id,
            "error": "missing_bind_values",
            "sql": sql_final,
            "questions": [
                f"Provide values for: {', '.join(sorted(missing))} or rephrase with explicit filters."
            ],
        }
        if include_debug:
            resp["debug"] = {
                "intent": intent,
                "prompt": prompt_text,
                "raw1": raw1,
                "validation": validation,
            }
        return jsonify(resp)

    exec_binds = {bind_name_map.get(k, k): v for k, v in bind_values.items()}
    log_event(
        log,
        "dw",
        "execution_binds",
        {"bind_names": sorted(exec_binds.keys())},
    )

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
        log_event(log, "dw", "oracle_error", {"error": error})

    duration_ms = int((datetime.utcnow() - started).total_seconds() * 1000)
    log_event(
        log,
        "dw",
        "execution_result",
        {"rows": len(rows), "cols": headers, "ms": duration_ms},
    )

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
        log_event(
            log,
            "dw",
            "inquiry_status",
            {"id": inq_id, "from": "open", "to": "failed", "reason": "oracle_error"},
        )
        return jsonify({"ok": False, "error": error, "inquiry_id": inq_id, "status": "failed"})

    csv_path = None
    if rows:
        csv_path = _write_csv(rows, headers)
        if csv_path:
            log_event(log, "dw", "csv_export", {"path": str(csv_path)})

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
        log_event(log, "dw", "snippet_saved", {"id": snippet_id})

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
    log_event(
        log,
        "dw",
        "inquiry_status",
        {"id": inq_id, "from": "open", "to": "answered", "rows": len(rows)},
    )

    binds_public = {
        bind_name_map.get(k, k): (
            v.isoformat() if hasattr(v, "isoformat") else v
        )
        for k, v in bind_values.items()
    }
    meta = {
        "rowcount": len(rows),
        "columns": headers,
        "duration_ms": duration_ms,
        "used_repair": bool(llm_out.get("used_repair")),
        "used_strict_retry": strict_attempted and bool(strict_raw),
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
        debug_payload = {
            "intent": intent,
            "prompt": prompt_text,
            "raw1": raw1,
            "validation": validation,
            "clarifier_raw": clarifier_raw,
        }
        if llm_out.get("used_repair"):
            debug_payload["raw2"] = llm_out.get("raw2")
        if strict_attempted:
            debug_payload["raw_strict"] = strict_raw
        resp["debug"] = debug_payload
    return jsonify(resp)


def create_dw_blueprint(*args, **kwargs):
    """Factory function returning the DocuWare blueprint."""
    return dw_bp
