from flask import Blueprint, request, jsonify, current_app
from sqlalchemy import text
from datetime import datetime, date, timedelta
import os, json, csv, pathlib, re, logging
from core.settings import Settings
from core.datasources import DatasourceRegistry
from core.sql_exec import get_mem_engine
from core.logging_setup import log_kv
from .llm import (
    clarify_intent,
    build_sql_prompt,
    build_sql_repair_prompt,
    nl_to_sql_raw,
    extract_sql,
)
from .validator import validate_sql, analyze_binds


dw_bp = Blueprint("dw", __name__, url_prefix="/dw")

NAMESPACE = os.environ.get("DW_NAMESPACE", "dw::common")
DW_DEBUG = os.environ.get("DW_DEBUG", "1") == "1"
DW_INCLUDE_DEBUG = os.environ.get("DW_INCLUDE_DEBUG", "1") == "1"


def _settings():
    return Settings()


def _get_allowed_columns() -> list:
    # The minimal set we agreed to start with
    return [
        "CONTRACT_ID", "CONTRACT_OWNER",
        "CONTRACT_STAKEHOLDER_1","CONTRACT_STAKEHOLDER_2","CONTRACT_STAKEHOLDER_3","CONTRACT_STAKEHOLDER_4",
        "CONTRACT_STAKEHOLDER_5","CONTRACT_STAKEHOLDER_6","CONTRACT_STAKEHOLDER_7","CONTRACT_STAKEHOLDER_8",
        "DEPARTMENT_1","DEPARTMENT_2","DEPARTMENT_3","DEPARTMENT_4","DEPARTMENT_5","DEPARTMENT_6","DEPARTMENT_7","DEPARTMENT_8",
        "OWNER_DEPARTMENT","CONTRACT_VALUE_NET_OF_VAT","VAT","CONTRACT_PURPOSE","CONTRACT_SUBJECT",
        "START_DATE","END_DATE","REQUEST_DATE","REQUEST_TYPE","CONTRACT_STATUS","ENTITY_NO","REQUESTER"
    ]


def _log(tag, payload):
    """Structured logging helper scoped to DW blueprint."""
    logger = current_app.logger if current_app else None
    if logger is None:
        # Fallback to root logger to avoid losing important traces
        logger = logging.getLogger(__name__)
    log_kv(logger, f"[dw] {tag}", payload)


def _question_has_window(q: str) -> bool:
    ql = (q or "").lower()
    return any(k in ql for k in [
        "last month", "next ", "last ", "between ", "in ", "since ", "days", "months", "year", "quarter"
    ])


def _guess_explicit_date_col(q: str) -> str | None:
    ql = (q or "").lower()
    if "end date" in ql or "expiry" in ql or "expires" in ql:
        return "END_DATE"
    if "start date" in ql:
        return "START_DATE"
    if "request date" in ql:
        return "REQUEST_DATE"
    return None


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
    allowed_cols = _get_allowed_columns()
    allow_binds = ["date_start","date_end","top_n","owner_name","dept","entity_no","contract_id_pattern","request_type"]

    # Create inquiry row (status open)
    with mem.begin() as conn:
        inq_id = conn.execute(text("""
            INSERT INTO mem_inquiries(namespace, question, auth_email, prefixes, status, created_at, updated_at)
            VALUES (:ns, :q, :email, CAST(:pfx AS jsonb), 'open', NOW(), NOW())
            RETURNING id
        """), {"ns": NAMESPACE, "q": q, "email": auth_email, "pfx": json.dumps(prefixes)}).scalar_one()

    _log("inquiry_start", {"id": inq_id, "q": q, "email": auth_email})

    # ---------- Clarify (non-blocking but useful) ----------
    intent = clarify_intent(q)
    _log("clarifier_raw", intent)
    intent_ok = bool(intent.get("ok")) and isinstance(intent.get("intent"), dict)
    has_window_by_clarifier = intent_ok and bool(intent["intent"].get("has_time_window"))
    explicit_dates = intent_ok and intent["intent"].get("explicit_dates") or None
    date_col_hint = intent_ok and intent["intent"].get("date_column") or None
    top_n_hint = intent_ok and intent["intent"].get("top_n") or None

    # ---------- Derive binds from question (or clarifier) ----------
    has_window_by_phrase = _question_has_window(q)
    window_binds = {}
    if explicit_dates and "date_start" in explicit_dates and "date_end" in explicit_dates:
        try:
            window_binds = {
                "date_start": datetime.fromisoformat(explicit_dates["date_start"]),
                "date_end": datetime.fromisoformat(explicit_dates["date_end"]),
            }
        except Exception:
            window_binds = {}
    if not window_binds and (has_window_by_phrase or has_window_by_clarifier):
        window_binds = _derive_dates_for_question(q)

    # Which date column should be used if a window is requested?
    suggested_date_col = _guess_explicit_date_col(q) or date_col_hint
    question_has_window = bool(explicit_dates) or has_window_by_phrase or has_window_by_clarifier

    # TOP N literal to avoid bind oddities in FETCH FIRST
    top_n_literal = None
    m_top = re.search(r"top\s+(\d+)", q.lower())
    if m_top:
        top_n_literal = int(m_top.group(1))
    elif isinstance(top_n_hint, int):
        top_n_literal = top_n_hint

    # ---------- Build prompt & first pass ----------
    prompt = build_sql_prompt(
        q,
        table_name=table_name,
        allowed_columns=allowed_cols,
        allowed_binds=allow_binds,
        default_date_column=default_date_col,
        force_date_binds=question_has_window,
        suggested_date_column=suggested_date_col,
        top_n_literal=top_n_literal,
    )
    _log("sql_prompt", {"prompt": prompt})
    raw1 = nl_to_sql_raw(prompt)
    _log("llm_raw_pass1", {"text": raw1})
    sql1 = extract_sql(raw1) or ""
    _log("llm_sql_pass1", {"sql": sql1})

    # ---------- Validate ----------
    v1 = validate_sql(
        sql1,
        allow_tables=[table_name],
        allow_columns=allowed_cols,
        allow_binds=allow_binds,
        question_has_window=question_has_window,
        required_date_column=(suggested_date_col or default_date_col) if question_has_window else None,
    )
    _log("validation_pass1", v1)

    # ---------- Repair pass if needed ----------
    sql_final = sql1
    v_final = v1
    used_repair = False
    if not v1["ok"]:
        used_repair = True
        repair_prompt = build_sql_repair_prompt(
            q, sql1, v1["errors"],
            table_name=table_name,
            allowed_columns=allowed_cols,
            allowed_binds=allow_binds,
            default_date_column=default_date_col,
            suggested_date_column=suggested_date_col,
            top_n_literal=top_n_literal,
        )
        _log("sql_repair_prompt", {"prompt": repair_prompt})
        raw2 = nl_to_sql_raw(repair_prompt)
        _log("llm_raw_pass2", {"text": raw2})
        sql2 = extract_sql(raw2) or ""
        _log("llm_sql_pass2", {"sql": sql2})
        v2 = validate_sql(
            sql2,
            allow_tables=[table_name],
            allow_columns=allowed_cols,
            allow_binds=allow_binds,
            question_has_window=question_has_window,
            required_date_column=(suggested_date_col or default_date_col) if question_has_window else None,
        )
        _log("validation_pass2", v2)
        if v2["ok"]:
            sql_final = sql2
            v_final = v2

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
            "sql": sql_final or prompt,  # show what we tried
            "questions": [
                "I couldn't derive a clean SELECT. Can you rephrase or specify filters (stakeholders, departments, date columns)?"
            ],
        }
        if include_debug:
            res["debug"] = {
                "clarifier": intent,
                "prompt": prompt,
                "raw1": raw1,
                "sql1": sql1,
                "validation1": v1,
                "used_repair": used_repair,
            }
        return jsonify(res)

    # ---------- Prepare binds ----------
    binds = {}
    if question_has_window:
        found_binds = set(v_final["binds"])
        if {"date_start", "date_end"}.issubset(found_binds):
            if window_binds.get("date_start") and window_binds.get("date_end"):
                binds.update(window_binds)
            else:
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
                        "clarifier": intent,
                        "prompt": prompt,
                        "raw1": raw1,
                        "sql1": sql1,
                        "validation1": v1,
                        "used_repair": used_repair,
                    }
                return jsonify(res)
        else:
            with mem.begin() as conn:
                conn.execute(
                    text(
                        """
                    UPDATE mem_inquiries
                       SET status='needs_clarification', last_sql=:sql, last_error='missing_binds', updated_at=NOW()
                     WHERE id=:id
                """
                    ),
                    {"sql": sql_final, "id": inq_id},
                )
            res = {
                "ok": False,
                "status": "needs_clarification",
                "inquiry_id": inq_id,
                "error": "missing_binds",
                "sql": sql_final,
                "questions": [
                    "The question implies a time window. Provide :date_start/:date_end or rephrase with explicit dates."
                ],
            }
            if include_debug:
                res["debug"] = {
                    "clarifier": intent,
                    "prompt": prompt,
                    "raw1": raw1,
                    "sql1": sql1,
                    "validation1": v1,
                    "used_repair": used_repair,
                }
            return jsonify(res)

    bind_info = analyze_binds(sql_final, allow_binds, provided=binds)
    _log("bind_analysis", bind_info)
    if bind_info["unknown"]:
        with mem.begin() as conn:
            conn.execute(
                text(
                    """
                UPDATE mem_inquiries
                   SET status='needs_clarification', last_sql=:sql, last_error='illegal_bind', updated_at=NOW()
                 WHERE id=:id
            """
                ),
                {"sql": sql_final, "id": inq_id},
            )
        return jsonify(
            {
                "ok": False,
                "status": "needs_clarification",
                "inquiry_id": inq_id,
                "error": "illegal_bind",
                "sql": sql_final,
                "questions": [
                    "Unsupported bind(s) detected. Please rephrase or remove custom binds."
                ],
            }
        )

    if bind_info["missing"]:
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
                f"Provide values for: {', '.join(bind_info['missing'])} or rephrase with explicit filters."
            ],
        }
        if include_debug:
            res["debug"] = {
                "clarifier": intent,
                "prompt": prompt,
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
        _log("oracle_error", {"error": error})

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
        "suggested_date_column": suggested_date_col or default_date_col,
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
            "clarifier": intent,
            "prompt": prompt,
            "raw1": raw1,
            "validation1": v1,
            "used_repair": used_repair,
        }
    return jsonify(resp)


def create_dw_blueprint(*args, **kwargs):
    """Factory function returning the DocuWare blueprint."""
    return dw_bp
