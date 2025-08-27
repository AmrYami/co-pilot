"""
Flask blueprint for FrontAccounting endpoints.

Routes
------
POST /fa/ingest
  { "prefixes": ["2_", "3_"], "fa_version": "2.4.17" }
  -> { "snapshots": {"2_": 12, "3_": 13} }

POST /fa/answer
  { "prefixes": ["2_"], "question": "top 10 customers by sales last month" }
  -> { sql, rationale, status, context?, questions? }

All routes expect prefixes to match ^[0-9]+_$. We do not pre-approve prefixes; any
well-formed prefix is accepted.
"""
from __future__ import annotations

import json
import re
from typing import Any, Iterable, List

from flask import Blueprint, current_app, jsonify, request
from core.inquiries import create_or_update_inquiry
from core.pipeline import Pipeline

from core.settings import Settings
from apps.fa.adapters import expand_keywords
from apps.fa.config import FAConfig, get_metrics
from core.inquiries import set_feedback
from core.alerts import queue_alert, notify_admins_via_email
from core.sql_exec import validate_select, explain, run_select, as_csv
from core.mailer import send_email_with_attachments
from core.agents import ValidatorAgent

fa_bp = Blueprint("fa", __name__)
PREFIX_RE = re.compile(r"^[0-9]+_$")


def _get_pipeline():
    # We standardize on app.config["PIPELINE"]; main.py should set it.
    return current_app.config["PIPELINE"]


def _validate_prefixes(prefixes: Iterable[str]) -> List[str]:
    ps = list(prefixes)
    if not ps:
        raise ValueError("prefixes is required and must be a non-empty array")
    for p in ps:
        if not PREFIX_RE.match(p):
            raise ValueError(f"Invalid prefix: {p}")
    return ps

@fa_bp.post("/run")
def run_query():
    """
    Execute a SELECT/CTE against FA (safe runner).
    Body:
      {
        "prefixes": ["579_"],
        "question": "top 10 customers last month",  # optional when sql provided
        "sql": "...",                               # optional; canonical or prefixed
        "limit": 500,
        "email": true,
        "auth_email": "user@example.com"
      }
    """
    data = request.get_json(force=True) or {}
    prefixes = data.get("prefixes") or []
    sql_in = (data.get("sql") or "").strip()
    question = (data.get("question") or "").strip()
    limit = int(data.get("limit") or 500)
    do_email = bool(data.get("email", False))
    auth_email = data.get("auth_email") or current_app.config["SETTINGS"].get("AUTH_EMAIL")

    pipeline: Pipeline = current_app.config["PIPELINE"]
    if isinstance(pipeline.settings, Settings) and prefixes:
        pipeline.settings.set_namespace(f"fa::{prefixes[0]}")

    if not sql_in:
        plan = pipeline.answer(source="fa", prefixes=prefixes, question=question)
        if plan.get("status") != "ok":
            return jsonify({"error": "planning_failed", "detail": plan}), 400
        sql_in = plan["sql"]

    from core.pipeline import SQLRewriter
    sql_exec = SQLRewriter.rewrite_for_prefixes(sql_in, prefixes) if prefixes else sql_in

    ok, msg = validate_select(sql_exec)
    if not ok:
        return jsonify({"error": msg}), 400

    fa_engine = pipeline.fa_engine
    if not fa_engine:
        return jsonify({"error": "FA DB not configured"}), 500

    try:
        explain(fa_engine, sql_exec)
        result = run_select(fa_engine, sql_exec, limit=limit)
    except Exception as e:
        return jsonify({"error": str(e)}), 400

    if do_email:
        s = current_app.config["SETTINGS"]
        try:
            send_email_with_attachments(
                smtp_host=s.get("SMTP_HOST", "localhost"),
                smtp_port=int(s.get("SMTP_PORT", "465") or 465),
                smtp_user=s.get("SMTP_USER"),
                smtp_password=s.get("SMTP_PASSWORD"),
                mail_from=s.get("SMTP_FROM", "no-reply@example.com"),
                to=[auth_email] if auth_email else [],
                subject="[Copilot] Your data export",
                body_text="Your CSV export is attached.",
                attachments=[("result.csv", as_csv(result), "text/csv")]
            )
            return jsonify({"ok": True, "emailed_to": auth_email, "rowcount": result["rowcount"]})
        except Exception as e:
            return jsonify({"error": f"email failed: {e}", "result": result}), 200

    return jsonify({"ok": True, "result": result})

@fa_bp.post("/ingest")
def ingest_prefixes():  # type: ignore[no-redef]
    try:
        data: dict[str, Any] = request.get_json(force=True) or {}
        prefixes = _validate_prefixes(data.get("prefixes", []))
        fa_version = data.get("fa_version")

        pipeline = _get_pipeline()
        # Set active namespace to the first tenant for settings lookups
        ns = f"fa::{prefixes[0]}"
        if isinstance(pipeline.settings, Settings):
            pipeline.settings.set_namespace(ns)

        # Ensure FA URL is present via settings; fail fast if missing
        facfg = FAConfig.from_settings(pipeline.settings)
        if not facfg.db_url:
            return jsonify({"error": "FA_DB_URL not configured"}), 400

        snaps = pipeline.ensure_ingested("fa", prefixes, fa_version=fa_version)
        return jsonify({"snapshots": snaps})
    except Exception as e:
        return jsonify({"error": str(e)}), 400

from apps.fa.hints import make_fa_hints

@fa_bp.post("/answer")
def answer():
    data = request.get_json(force=True)
    prefixes = data.get("prefixes", [])
    question = (data.get("question") or "").strip()
    auth_email = data.get("auth_email") or current_app.config["SETTINGS"].get("AUTH_EMAIL")

    pipeline: Pipeline = current_app.config["PIPELINE"]
    s = current_app.config["SETTINGS"]

    # ensure namespace for settings resolution
    if isinstance(pipeline.settings, Settings) and prefixes:
        pipeline.settings.set_namespace(f"fa::{prefixes[0]}")

    # 1) Try to answer
    result = pipeline.answer(
        source="fa",
        prefixes=prefixes,
        question=question
    )

    # 2) Decide the user-facing status
    status = "answered" if result.get("status") == "ok" else "awaiting_admin"

    # 3) Create inquiry row
    try:
        inquiry_id = create_or_update_inquiry(
            current_app.config["MEM_ENGINE"],
            namespace=f"fa::{prefixes[0]}" if prefixes else "fa::common",
            prefixes=prefixes,
            question=question,
            auth_email=auth_email,
            run_id=None,
            research_enabled=bool(s.get("RESEARCH_MODE", False)),
            status=("answered" if result.get("status") == "ok" else "awaiting_admin"),
            research_summary=None,
            source_ids=None
        )
    except Exception as e:
        # don't fail user response if audit insert fails
        inquiry_id = None
        result.setdefault("warnings", []).append(f"inquiry_log_failed: {e}")

    # 4) If we failed to produce final SQL, escalate to admins immediately
    if result.get("status") != "ok":
        try:
            admin_list = s.get("ALERTS_EMAILS") or s.get("ADMIN_EMAILS") or []
            if isinstance(admin_list, str):
                admin_list = [x.strip() for x in admin_list.split(",") if x.strip()]

            if admin_list:
                ns = f"fa::{prefixes[0]}" if prefixes else "fa::common"
                tables_ = [t.get("table_name") for t in (result.get("context", {}).get("tables") or [])][:6]
                cols_ = [f"{c.get('table_name')}.{c.get('column_name')}" for c in (result.get("context", {}).get("columns") or [])][:10]

                reply_template = {
                    "answered_by": "admin@example.com",
                    "admin_reply": "Describe the measure, correct date column, joins & filters in words.",
                    "sql": "SELECT ...",  # optional; canonical or already-prefixed (SELECT/CTE only)
                    "persist": { "rules": [], "mappings": [], "glossary": [] }
                }

                subject = f"[Copilot] Clarification needed — {ns} — Inquiry #{inquiry_id or 'N/A'}"
                body = (
                    f"Namespace: {ns}\n"
                    f"Prefixes: {', '.join(prefixes) if prefixes else '(none)'}\n"
                    f"Question: {question}\n\n"
                    f"Matched tables: {', '.join(tables_) if tables_ else '(none)'}\n"
                    f"Matched columns: {', '.join(cols_) if cols_ else '(none)'}\n\n"
                    f"Copilot follow-ups (for admin):\n"
                    + ("\n".join([f"- {q}" for q in (result.get('questions') or [])]) or "(none)") + "\n\n"
                    f"Reply API (POST): /admin/inquiries/{inquiry_id or 'N/A'}/reply\n"
                    f"Headers: X-Admin-Key: <your-admin-key>\n"
                    f"JSON body template:\n{json.dumps(reply_template, indent=2)}\n"
                )

                notify_admins_via_email(
                    subject=subject,
                    body_text=body,
                    to_emails=admin_list
                )
            else:
                result.setdefault("warnings", []).append("ALERTS_EMAILS/ADMIN_EMAILS is empty; no admin email sent")
        except Exception as e:
            result.setdefault("warnings", []).append(f"admin_email_failed: {e}")

    # 5) Return minimal payload to end user (no questions)
    if result.get("status") == "ok":
        # you can include sql/rationale here if you want; leaving as is
        return jsonify({
            "status": "ok",
            "sql": result.get("sql"),
            "rationale": result.get("rationale"),
            "inquiry_id": inquiry_id
        })

    # Waiting on admins
    return jsonify({
        "status": "awaiting_admin",
        "message": "We’re preparing your data. Our admins will clarify and you’ll receive the result by email.",
        "inquiry_id": inquiry_id
    })

@fa_bp.get("/metrics")
def list_metrics():
    pipeline = _get_pipeline()
    m = get_metrics(pipeline.settings)
    return jsonify({"metrics": m.get("metrics", {})})


@fa_bp.post("/feedback")
def feedback():
    """
    Save user satisfaction feedback for a previous inquiry.
    Body:
      { "inquiry_id": 123, "satisfied": true, "rating": 5, "feedback_comment": "Great" }
    """
    data = request.get_json(force=True) or {}
    inq_id = int(data.get("inquiry_id", 0))
    if not inq_id:
        return jsonify({"error": "inquiry_id required"}), 400
    set_feedback(
        current_app.config["MEM_ENGINE"],
        inquiry_id=inq_id,
        satisfied=bool(data.get("satisfied")),
        rating=data.get("rating"),
        comment=data.get("feedback_comment"),
    )
    return jsonify({"ok": True, "inquiry_id": inq_id})
