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


@fa_bp.post("/answer")
def answer():
    """
    Handle a user question:
      - Build context and plan via pipeline
      - Normalize status
      - Persist a mem_inquiries row
      - If auto-escalation is enabled and we need clarification, email admins

    Body:
      { "prefixes": ["579_"], "question": "..." , "auth_email": "user@example.com" }

    Returns:
      { "status": "...", "context": {...}, "questions": [...], "inquiry_id": 123, ... }
    """
    data = request.get_json(force=True) or {}
    prefixes = data.get("prefixes", [])
    question = (data.get("question") or "").strip()
    auth_email = data.get("auth_email") or current_app.config["SETTINGS"].get("AUTH_EMAIL")

    pipeline: Pipeline = current_app.config["PIPELINE"]

    # Set namespace for settings lookups based on first prefix
    if isinstance(pipeline.settings, Settings) and prefixes:
        pipeline.settings.set_namespace(f"fa::{prefixes[0]}")

    # 1) Ask the pipeline
    result = pipeline.answer(source="fa", prefixes=prefixes, question=question)
    warnings: list[str] = result.setdefault("warnings", [])

    # 2) Normalize status from pipeline
    #    pipeline can return: needs_clarification | needs_fix | escalated | ok
    status = "open"
    rstatus = (result.get("status") or "").lower()
    if rstatus == "ok":
        status = "answered"
    elif rstatus == "escalated":
        status = "awaiting_admin"
    elif rstatus in ("needs_fix", "needs_clarification"):
        status = "open"

    # 3) Apply auto-escalation on clarification if configured
    s = current_app.config["SETTINGS"]
    auto_escalate = bool(s.get("AUTO_ESCALATE_ON_CLARIFICATION", False))
    if rstatus == "needs_clarification" and auto_escalate:
        status = "awaiting_admin"

    # Keep the final status in the API response too
    result["status"] = status

    # 4) Persist one row into mem_inquiries and capture its id
    try:
        inquiry_id = create_or_update_inquiry(
            current_app.config["MEM_ENGINE"],
            namespace=f"fa::{prefixes[0]}" if prefixes else "fa::common",
            prefixes=prefixes,
            question=question,
            auth_email=auth_email,
            run_id=None,
            research_enabled=bool(s.get("RESEARCH_MODE", False)),
            status=status
        )
        result["inquiry_id"] = inquiry_id
    except Exception as e:
        warnings.append(f"inquiry_log_failed: {e}")
        inquiry_id = None  # still try to email if needed (subject will show N/A)

    # 5) If we’re awaiting_admin, email admins now
    if status == "awaiting_admin":
        try:
            admin_list = s.get("ALERTS_EMAILS") or s.get("ADMIN_EMAILS") or []
            # allow comma-separated strings from env as well
            if isinstance(admin_list, str):
                admin_list = [x.strip() for x in admin_list.split(",") if x.strip()]

            if admin_list:
                ns = f"fa::{prefixes[0]}" if prefixes else "fa::common"
                ctx = result.get("context") or {}
                tables_ = [t.get("table_name") for t in (ctx.get("tables") or [])][:5]
                cols_ = [f"{c.get('table_name')}.{c.get('column_name')}" for c in (ctx.get("columns") or [])][:8]

                # Tiny Postman-ready JSON template for admin reply endpoint
                reply_template = {
                    "answered_by": "admin@example.com",
                    "admin_reply": "Explanation or notes",
                    "sql": "SELECT ...",  # canonical or prefixed; SELECT/CTE only
                    "persist": {"rules": [], "mappings": [], "glossary": []}
                }

                subject = f"[Copilot] Clarification needed for {ns} — Inquiry #{inquiry_id or 'N/A'}"
                body = (
                    f"Namespace: {ns}\n"
                    f"Prefixes: {', '.join(prefixes) if prefixes else '(none)'}\n"
                    f"Question: {question}\n\n"
                    f"Top matched tables: {', '.join(tables_) if tables_ else '(none)'}\n"
                    f"Top matched columns: {', '.join(cols_) if cols_ else '(none)'}\n\n"
                    f"Follow-up questions from copilot:\n"
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
                result["escalated"] = True
            else:
                warnings.append("ALERTS_EMAILS/ADMIN_EMAILS empty; no admin email sent")
        except Exception as e:
            warnings.append(f"admin_email_failed: {e}")

    return jsonify(result)

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
