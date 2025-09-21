from __future__ import annotations

import csv
import json
import os
import pathlib
from collections.abc import Mapping
from datetime import date, datetime, timedelta
from typing import Optional

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


SQL_ONLY_STRICT_TEMPLATE = """Write only an Oracle query. Start with SELECT or WITH.
No code fences. No comments. No explanations. No extra text.
Table: "{table}"
Allowed columns: {allowed_columns}
Allowed binds: {allowed_binds}
Use Oracle syntax. For a time window, filter with :date_start and :date_end on {date_column}.
Question:
{question}
"""


def _env_truthy(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() not in {"", "0", "false", "no"}


NAMESPACE = os.environ.get("DW_NAMESPACE", "dw::common")
DW_INCLUDE_DEBUG = _env_truthy("DW_INCLUDE_DEBUG", default=True)


def _as_dict(value) -> dict:
    return value if isinstance(value, Mapping) else {}


def _llm_out_default() -> dict:
    return {"prompt": "", "raw1": "", "raw2": "", "raw_strict": "", "errors": []}


def _settings():
    return Settings()


def _heuristic_fill(question: str, intent: dict, default_date_col: str) -> dict:
    if not isinstance(intent, dict):
        return {}

    lowered = (question or "").lower()
    upper = (question or "").upper()

    if intent.get("has_time_window") is None:
        if "next 30 day" in lowered or "next thirty day" in lowered:
            intent["has_time_window"] = True

    if intent.get("date_column") is None:
        if "END_DATE" in upper:
            intent["date_column"] = "END_DATE"
        elif "START_DATE" in upper:
            intent["date_column"] = "START_DATE"
        elif "REQUEST_DATE" in upper:
            intent["date_column"] = "REQUEST_DATE"
        elif intent.get("has_time_window"):
            intent["date_column"] = default_date_col

    if intent.get("explicit_dates") is None:
        if "next 30 day" in lowered or "next thirty day" in lowered:
            today = date.today()
            intent["explicit_dates"] = {
                "start": today.isoformat(),
                "end": (today + timedelta(days=30)).isoformat(),
            }

    if intent.get("explicit_dates") and intent.get("has_time_window") is None:
        intent["has_time_window"] = True

    if intent.get("has_time_window") and intent.get("date_column") is None:
        intent["date_column"] = default_date_col

    return intent


def _synthesize_window_query(table: str, date_col: str, top_n: Optional[int] = None) -> str:
    table_literal = table.strip() or "Contract"
    if not table_literal.startswith('"') or not table_literal.endswith('"'):
        table_literal = f'"{table_literal.strip("\"")}"'

    base = f"""
SELECT
  CONTRACT_ID,
  CONTRACT_OWNER,
  {date_col} AS WINDOW_DATE,
  CONTRACT_VALUE_NET_OF_VAT
FROM {table_literal}
WHERE {date_col} BETWEEN :date_start AND :date_end
ORDER BY {date_col} ASC
""".strip()

    if top_n is not None:
        try:
            top_val = int(top_n)
        except Exception:
            top_val = None
        if top_val and top_val > 0:
            base = f"{base}\nFETCH FIRST {top_val} ROWS ONLY"

    return base


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

    intent = _heuristic_fill(q, intent, default_date_col)

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

    if intent.get("explicit_dates") and intent.get("has_time_window") is None:
        intent["has_time_window"] = True
    if intent.get("has_time_window") and intent.get("date_column") is None:
        intent["date_column"] = default_date_col

    log_event(log, "dw", "clarifier_intent_adjusted", json.loads(json.dumps(intent, default=str)))
    if clarifier_raw and include_debug:
        log_event(
            log,
            "dw",
            "clarifier_raw_debug",
            {"size": len(clarifier_raw), "text": clarifier_raw[:900]},
        )

    try:
        llm_out = nl_to_sql_with_llm(q, ctx, intent=intent)
    except Exception as exc:  # pragma: no cover - defensive guard
        log.exception("dw nl_to_sql_with_llm failed")
        llm_out = {"errors": [f"llm_generate:{type(exc).__name__}:{exc}"]}

    d = _as_dict(llm_out) or _llm_out_default()
    prompt_text = d.get("prompt", "") or ""
    raw1 = d.get("raw1", "") or ""
    raw2 = d.get("raw2", "") or ""
    raw_strict_hint = d.get("raw_strict", "") or ""

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
        strict_prompt = SQL_ONLY_STRICT_TEMPLATE.format(
            table=table_name,
            allowed_columns=", ".join(ALLOWED_COLUMNS),
            allowed_binds=", ".join(ALLOWED_BINDS),
            date_column=intent.get("date_column") or default_date_col,
            question=q,
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

    def _maybe_synthesize(reason: str) -> str:
        explicit = intent.get("explicit_dates") if isinstance(intent.get("explicit_dates"), dict) else None
        if not explicit:
            return ""
        start = explicit.get("start")
        end = explicit.get("end")
        if not (start and end):
            return ""
        date_col_for_window = intent.get("date_column") or default_date_col
        if not date_col_for_window:
            return ""
        if date_col_for_window not in ALLOWED_COLUMNS:
            return ""
        table_clean = table_name.strip().strip('"')
        if table_clean.lower() != "contract":
            return ""
        top_n_val = intent.get("top_n")
        top_n_num: Optional[int] = None
        if top_n_val is not None:
            try:
                top_n_num = int(top_n_val)
            except Exception:
                top_n_num = None
        synth_sql = _synthesize_window_query(table_name, date_col_for_window, top_n_num)
        if synth_sql:
            log_event(
                log,
                "dw",
                "synthetic_sql_fallback",
                {
                    "reason": reason,
                    "table": table_name,
                    "date_column": date_col_for_window,
                    "start": start,
                    "end": end,
                    "top_n": top_n_num,
                },
            )
        return synth_sql

    sql_from_llm = d.get("sql") or ""
    sql_final = ""
    candidates = [
        (raw_strict_hint, raw1 or raw2 or sql_from_llm),
        (raw1, raw2 or sql_from_llm),
        (raw2, raw1 or sql_from_llm),
        (sql_from_llm, raw1 or raw2),
    ]
    for primary, fallback in candidates:
        if not primary and not fallback:
            continue
        sql_final = sanitize_oracle_sql(primary, fallback)
        if sql_final:
            break
    if not sql_final:
        strict_raw = _strict_retry()
        if strict_raw:
            raw_strict_hint = strict_raw
            sql_final = sanitize_oracle_sql(strict_raw, raw1 or raw2 or sql_from_llm)
    if not sql_final:
        sql_final = _maybe_synthesize("empty_sanitize")
    if not sql_final:
        sql_payload = {"size": 0}
        sql_payload["sql"] = "" if include_debug else "<hidden>"
        log_event(log, "dw", "final_sql", sql_payload)
        with mem.begin() as conn:
            conn.execute(
                text(
                    """
                UPDATE mem_inquiries
                   SET status = 'needs_clarification',
                       last_sql = :sql,
                       last_error = 'no_sql_extracted',
                       updated_at = NOW()
                 WHERE id = :id
            """
                ),
                {"sql": "", "id": inq_id},
            )
        log_event(
            log,
            "dw",
            "inquiry_status",
            {"id": inq_id, "from": "open", "to": "needs_clarification", "reason": "no_sql_extracted"},
        )
        res = {
            "ok": False,
            "status": "needs_clarification",
            "inquiry_id": inq_id,
            "error": "no_sql_extracted",
            "sql": "",
            "questions": [
                "I couldn't extract a SELECT statement. Can you restate the request with the date column and time window?",
            ],
        }
        if include_debug:
            debug_payload = {
                "intent": intent,
                "prompt": prompt_text,
                "raw1": raw1,
                "clarifier_raw": clarifier_raw,
            }
            if raw2:
                debug_payload["raw2"] = raw2
            strict_debug = strict_raw if strict_attempted else raw_strict_hint
            if strict_debug:
                debug_payload["raw_strict"] = strict_debug
            errors = d.get("errors") or []
            if errors:
                debug_payload["errors"] = errors
            res["debug"] = debug_payload
        return jsonify(res)

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
            raw_strict_hint = strict_raw
            alt_sql = sanitize_oracle_sql(strict_raw, raw1 or raw2 or sql_from_llm)
            if alt_sql:
                sql_final = alt_sql
                parse_error = _oracle_parse_error(sql_final)
    if parse_error:
        synthesized = _maybe_synthesize("parse_error")
        if synthesized:
            sql_final = synthesized
            parse_error = _oracle_parse_error(sql_final)

    sql_payload = {"size": len(sql_final)}
    sql_payload["sql"] = sql_final[:900] if include_debug else "<hidden>"
    log_event(log, "dw", "final_sql", sql_payload)
    validation = d.get("validation") or basic_checks(sql_final, allowed_binds=ALLOWED_BINDS)
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
            strict_debug = strict_raw if strict_attempted else raw_strict_hint
            if strict_debug:
                debug_payload["raw_strict"] = strict_debug
            errors = d.get("errors") or []
            if errors:
                debug_payload["errors"] = errors
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
        "used_repair": bool(d.get("used_repair")),
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
        if d.get("used_repair"):
            debug_payload["raw2"] = d.get("raw2")
        strict_debug = strict_raw if strict_attempted else raw_strict_hint
        if strict_debug:
            debug_payload["raw_strict"] = strict_debug
        errors = d.get("errors") or []
        if errors:
            debug_payload["errors"] = errors
        resp["debug"] = debug_payload
    return jsonify(resp)


def create_dw_blueprint(*args, **kwargs):
    """Factory function returning the DocuWare blueprint."""
    return dw_bp
