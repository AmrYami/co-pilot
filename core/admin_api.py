"""
Admin endpoints for settings and resolving inquiries.

- POST /admin/inquiries/<id>/reply
  Body:
    {
      "answered_by": "ops@example.com",
      "admin_reply": "Try invoices total by customer last month",
      "sql": "SELECT ...",          # optional; if missing we try to derive from admin_reply
      "persist": { "rules": [], "mappings": [], "glossary": [] }  # optional future use
    }

Behavior:
  * If sql is provided: validate (EXPLAIN), execute, email CSV to auth_email, mark answered.
  * If sql is omitted: attempt to derive SQL from admin_reply via Planner; if success, same as above.
  * If still not solvable: keep inquiry in 'awaiting_admin', optionally re-notify admins with context.
"""
from __future__ import annotations
import hmac, json, os
from typing import Any, Dict, List, Optional

from flask import Blueprint, current_app, jsonify, request
from sqlalchemy import text

from io import StringIO
import csv, json, re

from core.inquiries import list_inquiries, mark_answered
from core.emailer import Emailer

from core.pipeline import SQLRewriter
from core.settings import Settings

from core.admin_helpers import derive_sql_from_admin_reply, verify_admin_key
from core.sql_exec import validate_select, explain, run_select, as_csv

from core.mailer import send_email_with_attachments, send_email
from core.alerts import notify_admins_via_email
from core.pipeline import Pipeline
from core.agents import ValidatorAgent

admin_bp = Blueprint("admin", __name__, url_prefix="/admin")


def send_inquiry_result_email(settings: Settings, to_email: str, subject: str, body_html: str, csv_bytes: bytes | None = None):
    mailer = Emailer(settings)
    atts = []
    if csv_bytes:
        atts.append(("result.csv", csv_bytes, "text/csv"))
    return mailer.send(
        to=[to_email],
        subject=subject,
        html=body_html,
        text=None,
        attachments=atts
    )


# settings_bp = Blueprint("settings", __name__)
admin_api = admin_bp

def _check_admin_key(req) -> bool:
    """Header-based admin auth using X-Admin-Key configured in env/DB."""
    supplied = (req.headers.get("X-Admin-Key") or "").strip()
    settings = current_app.config.get("SETTINGS")
    if not supplied or settings is None:
        return False
    return verify_admin_key(settings, supplied)

def _as_list(x) -> list[str]:
    if x is None:
        return []
    if isinstance(x, list):
        return [str(v).strip() for v in x if str(v).strip()]
    if isinstance(x, str):
        return [v.strip() for v in x.split(",") if v.strip()]
    return [str(x).strip()]

def _require_admin_key() -> Optional[str]:
    """Validate X-Admin-Key against SETTINGS_ADMIN_KEY hash/env."""
    supplied = (request.headers.get("X-Admin-Key") or "").strip()
    if not supplied:
        return "missing X-Admin-Key header"
    settings = current_app.config.get("SETTINGS")
    if not settings:
        return "server misconfigured: settings not available"
    if verify_admin_key(settings, supplied):
        return None
    return "invalid admin key"


def _auth_ok() -> bool:
    expected = os.getenv("SETTINGS_ADMIN_KEY") or os.getenv("ADMIN_API_KEY")
    provided = request.headers.get("X-Admin-Key")
    return bool(expected) and provided == expected

def _infer_type(val: Any) -> str:
    if isinstance(val, bool):   return "bool"
    if isinstance(val, int):    return "int"
    if isinstance(val, float):  return "float"
    if isinstance(val, (dict, list)): return "json"
    return "string"

def _pip():
    return current_app.config["PIPELINE"]

@admin_bp.post("/settings/bulk")
def settings_bulk():
    """
    Upsert multiple settings rows in mem_settings as JSONB.

    Body:
      {
        "namespace": "fa::common",
        "updated_by": "amr",
        "settings": [
          { "key": "AUTH_EMAIL", "value": "amr.yami1@gmail.com" },
          { "key": "ADMIN_EMAILS", "value": ["ops@example.com","dev@example.com"], "scope": "global" },
          ...
        ]
      }
    """
    err = _require_admin_key()
    if err:
        return jsonify({"error": err}), 401

    try:
        data = request.get_json(force=True) or {}
    except Exception as e:
        return jsonify({"error": f"invalid JSON: {e}"}), 400

    ns = (data.get("namespace") or "").strip()
    updated_by = (data.get("updated_by") or "").strip()
    items = data.get("settings")

    if not ns:
        return jsonify({"error": "namespace is required"}), 400
    if not updated_by:
        return jsonify({"error": "updated_by is required"}), 400
    if not isinstance(items, list):
        return jsonify({"error": "settings must be an array"}), 400

    eng = current_app.config.get("MEM_ENGINE")
    if eng is None:
        # pipeline sets this at app startup
        return jsonify({"error": "memory engine not available"}), 500

    try:
        with eng.begin() as con:
            for it in items:
                key = (it.get("key") or "").strip()
                if not key:
                    continue
                val = it.get("value")
                value_type = (it.get("value_type") or _infer_type(val)).lower()
                scope = (it.get("scope") or "namespace").lower()       # 'global'|'namespace'|'user'
                scope_id = it.get("scope_id")
                is_secret = bool(it.get("is_secret", False))
                category = it.get("category")
                description = it.get("description")
                val_json = json.dumps(val)

                # UPDATE first
                upd = con.execute(text("""
                    UPDATE mem_settings
                       SET value        = CAST(:value AS JSONB),
                           value_type   = :value_type,
                           is_secret    = :is_secret,
                           category     = :category,
                           description  = :description,
                           updated_by   = :updated_by,
                           updated_at   = NOW()
                     WHERE namespace = :ns
                       AND key       = :key
                       AND scope     = :scope
                       AND ((:scope_id IS NULL AND scope_id IS NULL) OR scope_id = :scope_id)
                """), {
                    "ns": ns,
                    "key": key,
                    "value": val_json,  # <-- REQUIRED
                    "value_type": value_type,
                    "is_secret": is_secret,
                    "scope": scope,
                    "scope_id": scope_id,
                    "category": category,
                    "description": description,
                    "updated_by": updated_by,
                })

                # INSERT if no row was updated
                if upd.rowcount == 0:
                    con.execute(text("""
                        INSERT INTO mem_settings(
                            namespace, key, value, value_type, is_secret,
                            scope, scope_id, category, description,
                            overridable, updated_by, created_at, updated_at
                        )
                        VALUES (
                            :ns, :key, CAST(:value AS JSONB), :value_type, :is_secret,
                            :scope, :scope_id, :category, :description,
                            TRUE, :updated_by, NOW(), NOW()
                        )
                    """), {
                        "ns": ns,
                        "key": key,
                        "value": val_json,  # <-- REQUIRED
                        "value_type": value_type,
                        "is_secret": is_secret,
                        "scope": scope,
                        "scope_id": scope_id,
                        "category": category,
                        "description": description,
                        "updated_by": updated_by,
                    })

        return jsonify({"ok": True})
    except Exception as e:
        # Surface as client-visible 400 with message instead of 500
        return jsonify({"error": str(e)}), 400

@admin_bp.get("/settings/get")
def settings_get():
    """
    Fetch selected keys (first match by precedence) for a namespace.
    /admin/settings/get?namespace=fa::common&keys=ASK_MODE,RESEARCH_MODE
    """
    err = _require_admin_key()
    if err:
        return jsonify({"error": err}), 401

    ns = request.args.get("namespace") or "default"
    keys_csv = request.args.get("keys") or ""
    keys = [k.strip() for k in keys_csv.split(",") if k.strip()]
    if not keys:
        return jsonify({"error": "keys query param required"}), 400

    s = current_app.config.get("SETTINGS")
    if s is None:
        return jsonify({"error": "settings not initialized"}), 500

    s.set_namespace(ns)
    out: Dict[str, Any] = {}
    for k in keys:
        out[k] = s.get(k)

    return jsonify({"namespace": ns, "values": out})

@admin_bp.get("/settings/summary")
def settings_summary():
    """
    Return cached settings snapshot for diagnostics (masked secrets).
    """
    err = _require_admin_key()
    if err:
        return jsonify({"error": err}), 401

    s = current_app.config.get("SETTINGS")
    if s is None:
        return jsonify({"error": "settings not initialized"}), 500
    return jsonify({"namespace": s._namespace, "summary": s.summary()})

@admin_bp.get("/inquiries")
def admin_list_inquiries():
    """
    List inquiries for a namespace, optionally filtered by status.
    Query: ?namespace=fa::common&status=awaiting_admin
    Auth: X-Admin-Key header must match SETTINGS_ADMIN_KEY
    """
    if not _check_admin_key(request):
        return jsonify({"error": "forbidden"}), 403

    ns = request.args.get("namespace", "fa::common")
    st = request.args.get("status")
    rows = list_inquiries(current_app.config["MEM_ENGINE"], namespace=ns, status=st, limit=100)
    return jsonify({"namespace": ns, "count": len(rows), "inquiries": rows})


@admin_bp.post("/inquiries/<int:inq_id>/reply")
def admin_reply_inquiry(inq_id: int):
    """
    Resolve an inquiry. `sql` is optional; if missing we try to derive it from admin_reply.
    On success: emails CSV (no SQL inside the email), marks answered.
    On failure: keeps status 'awaiting_admin' and (best-effort) re-notifies admins.
    """
    if not _check_admin_key(request):
        return jsonify({"error": "forbidden"}), 403

    data = request.get_json(force=True) or {}
    answered_by = (data.get("answered_by") or "").strip()
    admin_reply = (data.get("admin_reply") or "").strip()
    sql_in = (data.get("sql") or "").strip()
    if not answered_by:
        return jsonify({"error": "answered_by is required"}), 400
    if not admin_reply and not sql_in:
        return jsonify({"error": "Provide either sql or admin_reply"}), 400

    mem = current_app.config["MEM_ENGINE"]
    pipeline = current_app.config["PIPELINE"]
    app_engine = pipeline.app_engine
    if not app_engine:
        return jsonify({"error": "APP DB not configured"}), 500

    # Load inquiry
    with mem.connect() as con:
        inq = con.execute(text("SELECT * FROM mem_inquiries WHERE id=:id"), {"id": inq_id}).mappings().first()
    if not inq:
        return jsonify({"error": "inquiry not found"}), 404

    prefixes = inq.get("prefixes") or []
    namespace = inq.get("namespace") or "fa::common"
    auth_email = inq.get("auth_email")

    # If sql missing, attempt derivation from admin_reply using Planner
    derived_info = None
    if not sql_in:
        sql_derived, derived_info = derive_sql_from_admin_reply(pipeline, inq, admin_reply)
        if not sql_derived:
            # Still unsolved → keep awaiting_admin and re-notify admins with context + follow-ups
            _re_notify_admins(inq_id=inq_id, inq=inq, admin_reply=admin_reply, derived_info=derived_info)
            return jsonify({
                "ok": False,
                "status": "awaiting_admin",
                "reason": "Could not derive a valid query from admin_reply",
                "derived_info": derived_info or {}
            }), 200
        sql_in = sql_derived  # proceed with the derived SQL

    # Gate: only allow SELECT/CTE
    sql_strip = sql_in.lstrip(" (").strip()
    if not re.match(r"(?is)^(with|select)\b", sql_strip):
        return jsonify({"error": "Only SELECT/CTE queries are allowed"}), 400

    # Validate with EXPLAIN
    try:
        with app_engine.connect() as c:
            c.execute(text(f"EXPLAIN {sql_strip}"))
    except Exception as e:
        # Keep awaiting_admin and notify admins why it failed
        _re_notify_admins(inq_id=inq_id, inq=inq, admin_reply=admin_reply,
                          derived_info={"error": f"validation failed: {e}"})
        return jsonify({"ok": False, "status": "awaiting_admin", "error": f"validation failed: {e}"}), 200

    # Execute and build CSV (no LIMIT here; add one if you want to cap size)
    try:
        with app_engine.connect() as c:
            rs = c.execute(text(sql_strip))
            cols = list(rs.keys())
            sio = StringIO()
            w = csv.writer(sio)
            w.writerow(cols)
            for row in rs:
                w.writerow([row[c] for c in cols])
            csv_bytes = sio.getvalue().encode("utf-8")
    except Exception as e:
        _re_notify_admins(inq_id=inq_id, inq=inq, admin_reply=admin_reply,
                          derived_info={"error": f"execution failed: {e}"})
        return jsonify({"ok": False, "status": "awaiting_admin", "error": f"execution failed: {e}"}), 200

    # Email CSV to requester (no SQL in email)
    s = current_app.config["SETTINGS"]
    email_warning = None
    try:
        if auth_email:
            send_email_with_attachments(
                smtp_host=s.get("SMTP_HOST", "localhost"),
                smtp_port=int(s.get("SMTP_PORT", "465") or 465),
                smtp_user=s.get("SMTP_USER"),
                smtp_password=s.get("SMTP_PASSWORD"),
                mail_from=s.get("SMTP_FROM", "no-reply@example.com"),
                to=[auth_email],
                subject="[Copilot] Your data export is ready",
                body_text=(admin_reply or "Here is your requested data."),
                attachments=[("result.csv", csv_bytes, "text/csv")]
            )
    except Exception as e:
        email_warning = str(e)

    # Mark answered
    from core.inquiries import mark_answered
    mark_answered(mem, inquiry_id=inq_id, answered_by=answered_by, admin_reply=admin_reply)

    # (Optional) store snippet for later reuse — left as-is
    try:
        with mem.begin() as con:
            con.execute(text("""
                INSERT INTO mem_snippets(namespace, title, description, sql_template, sql_raw, is_verified, verified_by, created_at, updated_at)
                VALUES (:ns, :title, :desc, :tmpl, :raw, false, :by, NOW(), NOW())
            """), {
                "ns": namespace,
                "title": "Admin reply export",
                "desc": admin_reply or "Admin-provided query",
                "tmpl": sql_in,   # canonical (if admin provided), or derived canonical
                "raw": sql_strip, # executed SQL (prefixed already via planner)
                "by": answered_by
            })
    except Exception:
        pass

    resp = {"ok": True, "inquiry_id": inq_id, "emailed_to": auth_email}
    if email_warning:
        resp["email_warning"] = email_warning
    return jsonify(resp), 200

def _re_notify_admins(*, inq_id: int, inq, admin_reply: str, derived_info: dict | None):
    """
    Re-notify admins that the reply could not be executed/derived, including context & followups.
    """
    s = current_app.config["SETTINGS"]
    admin_list = _as_list(s.get("ALERTS_EMAILS") or s.get("ADMIN_EMAILS"))
    if not admin_list:
        return
    try:
        ns = inq.get("namespace") or "fa::common"
        prefixes = inq.get("prefixes") or []
        question = inq.get("question") or ""
        # Keep the email simple. No SQL in it.
        body = (
            f"Namespace: {ns}\n"
            f"Inquiry #{inq_id}\n"
            f"Prefixes: {', '.join(prefixes) if prefixes else '(none)'}\n"
            f"Question: {question}\n"
            f"Admin tried: {admin_reply or '(no notes)'}\n\n"
            f"Copilot still needs clarification.\n"
            f"Details: {json.dumps(derived_info or {}, ensure_ascii=False)}\n\n"
            f"Reply API: POST /admin/inquiries/{inq_id}/reply\n"
            f"Headers: X-Admin-Key: <your-admin-key>\n"
            f"Body keys: answered_by (req), admin_reply (opt), sql (opt)\n"
        )
        notify_admins_via_email(
            subject=f"[Copilot] Still needs clarification — Inquiry #{inq_id}",
            body_text=body,
            to_emails=admin_list
        )
    except Exception:
        # email failures are non-fatal
        pass

@admin_bp.post("/settings/test-email")
def test_email():
    if not _check_admin_key(request):
        return jsonify({"error": "forbidden"}), 403
    s = current_app.config["SETTINGS"]
    to = request.json.get("to") if request.is_json else None
    if not to:
        return jsonify({"error":"missing 'to'"}), 400
    from core.emailer import Emailer
    mailer = Emailer(s)
    r = mailer.send(
        to=[to],
        subject="Copilot SMTP test",
        html="<p>SMTP works ✅</p><p>Source: mem_settings / env</p>"
    )
    return jsonify(r)



