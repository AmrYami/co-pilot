from __future__ import annotations

import csv
import json
import os
import pathlib
import re
from calendar import monthrange
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
    extract_bind_names,
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


# --- Helpers to compute date ranges ---------------------------------------------------------
def _month_bounds(offset: int = 0, today: date | None = None) -> tuple[date, date]:
    """Return first/last day for month `today` + offset (offset=-1 -> last month)."""

    today = today or date.today()
    year, month = today.year, today.month + offset
    while month <= 0:
        month += 12
        year -= 1
    while month > 12:
        month -= 12
        year += 1
    start = date(year, month, 1)
    end = date(year, month, monthrange(year, month)[1])
    return start, end


def _quarter_bounds(offset: int = 0, today: date | None = None) -> tuple[date, date]:
    """Quarter bounds for quarter containing today + offset quarters."""

    today = today or date.today()
    quarter = (today.month - 1) // 3 + 1
    quarter += offset
    year = today.year + (quarter - 1) // 4
    quarter = ((quarter - 1) % 4) + 1
    month_start = 3 * (quarter - 1) + 1
    start = date(year, month_start, 1)
    end_month = month_start + 2
    end = date(year, end_month, monthrange(year, end_month)[1])
    return start, end


def derive_window_from_text(q: str) -> dict:
    """Best-effort parser for common date windows from free-form text."""

    lowered = (q or "").lower().strip()
    if not lowered:
        return {}

    today = date.today()

    match = re.search(r"\bnext\s+(\d{1,3})\s+days?\b", lowered)
    if match:
        days = int(match.group(1))
        return {
            "start": today.isoformat(),
            "end": (today + timedelta(days=days)).isoformat(),
        }

    match = re.search(r"\blast\s+(\d{1,3})\s+days?\b", lowered)
    if match:
        days = int(match.group(1))
        return {
            "start": (today - timedelta(days=days)).isoformat(),
            "end": today.isoformat(),
        }

    if "last month" in lowered:
        start, end = _month_bounds(-1, today)
        return {"start": start.isoformat(), "end": end.isoformat()}
    if "this month" in lowered or "current month" in lowered:
        start, end = _month_bounds(0, today)
        return {"start": start.isoformat(), "end": end.isoformat()}
    if "next month" in lowered:
        start, end = _month_bounds(+1, today)
        return {"start": start.isoformat(), "end": end.isoformat()}

    if "last quarter" in lowered:
        start, end = _quarter_bounds(-1, today)
        return {"start": start.isoformat(), "end": end.isoformat()}
    if "this quarter" in lowered or "current quarter" in lowered:
        start, end = _quarter_bounds(0, today)
        return {"start": start.isoformat(), "end": end.isoformat()}
    if "next quarter" in lowered:
        start, end = _quarter_bounds(+1, today)
        return {"start": start.isoformat(), "end": end.isoformat()}

    if "next 30 days" in lowered:
        return {
            "start": today.isoformat(),
            "end": (today + timedelta(days=30)).isoformat(),
        }

    return {}


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

    upper = (question or "").upper()

    derived_window = derive_window_from_text(question or "")
    if derived_window and not intent.get("explicit_dates"):
        intent["explicit_dates"] = derived_window

    if intent.get("has_time_window") is None and derived_window:
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

    try:
        window_days = int(body.get("window_days") or 0)
    except (TypeError, ValueError):
        window_days = 0
    date_column_override = (body.get("date_column") or "").upper().strip()
    override_explicit_dates = None
    override_date_column = None
    if window_days > 0:
        override_start = date.today()
        override_end = override_start + timedelta(days=window_days)
        override_explicit_dates = {
            "start": override_start.isoformat(),
            "end": override_end.isoformat(),
        }
        override_date_column = date_column_override or "END_DATE"

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

    if override_explicit_dates:
        intent["explicit_dates"] = override_explicit_dates
        intent["has_time_window"] = True
        if override_date_column:
            intent["date_column"] = override_date_column
    elif date_column_override:
        intent["date_column"] = date_column_override

    intent = _heuristic_fill(q, intent, default_date_col)

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

    bind_names_in_sql = extract_bind_names(sql_final)
    bind_name_map = {name.lower(): name for name in bind_names_in_sql}

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
        "bind_names": sorted(bind_names_in_sql),
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

    needed_canonical = sorted(bind_name_map.keys())

    raw_bind_values = derive_bind_values(q, needed_canonical, intent) or {}
    bind_values: dict[str, object] = dict(raw_bind_values)

    needs_dates = {"date_start", "date_end"} & set(needed_canonical)
    if needs_dates:
        window = {}
        if isinstance(intent, dict):
            maybe_window = intent.get("explicit_dates")
            if isinstance(maybe_window, dict):
                window = maybe_window
        if (
            (not window or not window.get("start") or not window.get("end"))
            and not (bind_values.get("date_start") and bind_values.get("date_end"))
        ):
            window = derive_window_from_text(q)

        if isinstance(window, dict) and window.get("start") and window.get("end"):
            def _coerce_date(value):
                if isinstance(value, datetime):
                    return value.date()
                if isinstance(value, date):
                    return value
                if isinstance(value, str):
                    try:
                        return date.fromisoformat(value)
                    except Exception:
                        return value
                return value

            bind_values["date_start"] = _coerce_date(window.get("start"))
            bind_values["date_end"] = _coerce_date(window.get("end"))

    if "top_n" in bind_name_map:
        top_n_val = None
        if isinstance(intent, dict):
            top_n_val = intent.get("top_n")
        if isinstance(top_n_val, int) and top_n_val > 0:
            bind_values["top_n"] = top_n_val

    missing = [
        name
        for name in needed_canonical
        if name not in bind_values or bind_values[name] is None
    ]
    if missing:
        missing_pretty = [bind_name_map.get(name, name) for name in missing]
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
                f"Provide values for: {', '.join(sorted(missing_pretty))} or rephrase with explicit filters."
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

    exec_binds = {
        bind_name_map.get(key, key): value
        for key, value in bind_values.items()
        if key in bind_name_map
    }
    log_event(log, "dw", "execution_binds", {k: str(v) for k, v in exec_binds.items()})

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

    rowcount = len(rows)

    binds_public = {
        bind_name_map.get(k, k): (
            v.isoformat() if hasattr(v, "isoformat") else v
        )
        for k, v in bind_values.items()
    }
    meta = {
        "rowcount": rowcount,
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
    if rowcount == 0:
        base_date_column = (intent.get("date_column") or "END_DATE").upper()
        suggestions = [
            {
                "action": "retry",
                "label": "Try next 60 days",
                "params": {"window_days": 60, "date_column": base_date_column},
            },
            {
                "action": "retry",
                "label": "Try next 90 days",
                "params": {"window_days": 90, "date_column": base_date_column},
            },
        ]
        if (intent.get("date_column") or "").upper() != "REQUEST_DATE":
            suggestions.append(
                {
                    "action": "retry",
                    "label": "Use REQUEST_DATE next 30 days",
                    "params": {"window_days": 30, "date_column": "REQUEST_DATE"},
                }
            )
        resp["note"] = "No contracts found in the selected window."
        resp["suggestions"] = suggestions
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
