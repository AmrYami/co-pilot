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

from core.admin_helpers import derive_sql_from_admin_reply
from core.mailer import send_email_with_attachments, send_email
from core.alerts import notify_admins_via_email
from core.pipeline import Pipeline


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
admin_bp = Blueprint("admin", __name__, url_prefix="/admin")
admin_api = admin_bp

def _check_admin_key(req) -> bool:
    """
    Verify X-Admin-Key header matches the configured admin key.
    Looks in Settings first, then ENV as a fallback.
    """
    incoming = (req.headers.get("X-Admin-Key") or "").strip()
    if not incoming:
        return False
    # Prefer DB-backed setting, fallback to env
    s = current_app.config.get("SETTINGS")
    expected = (s.get("SETTINGS_ADMIN_KEY") if s else None) or os.getenv("SETTINGS_ADMIN_KEY")
    if not expected:
        # if no key configured, deny by default
        return False
    return hmac.compare_digest(incoming, expected)

def _require_admin_key() -> Optional[str]:
    """Validate X-Admin-Key against SETTINGS_ADMIN_KEY env/env-loaded settings."""
    supplied = request.headers.get("X-Admin-Key")
    if not supplied:
        return "missing X-Admin-Key header"
    from os import getenv
    expected = getenv("SETTINGS_ADMIN_KEY")
    if not expected:
        return "server misconfigured: SETTINGS_ADMIN_KEY not set"
    if supplied != expected:
        return "invalid admin key"
    return None


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
    Resolve an inquiry:
      {
        "answered_by": "ops@example.com",
        "admin_reply": "Explanation...",
        "sql": "SELECT ...",                 # canonical (unprefixed) or already prefixed
        "persist": {                         # optional learning payloads (future hook)
          "rules": [...],
          "mappings": [...],
          "glossary": [...]
        }
      }
    Behavior:
      - EXPLAIN-only validation for safety.
      - If prefixes exist and SQL seems canonical, rewrite to prefixed SQL.
      - Execute and email CSV *only* (no SQL in email) to inquiry.auth_email.
      - Mark inquiry answered and stash a snippet record.
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
        return jsonify({"error": "Provide either admin_reply or sql"}), 400

    mem = current_app.config["MEM_ENGINE"]
    pipeline: Pipeline = current_app.config["PIPELINE"]
    fa_engine = pipeline.fa_engine
    if not fa_engine:
        return jsonify({"error": "FA DB not configured"}), 500

    with mem.connect() as con:
        inq = con.execute(text("SELECT * FROM mem_inquiries WHERE id=:id"), {"id": inq_id}).mappings().first()
    if not inq:
        return jsonify({"error": "inquiry not found"}), 404

    prefixes = inq["prefixes"] or []
    namespace = inq["namespace"]
    auth_email = inq["auth_email"]

    sql_exec = None
    validation_info = None

    if sql_in:
        # Safety: SELECT/CTE only
        sql_strip = sql_in.lstrip(" (").strip()
        if not re.match(r"(?is)^(with|select)\b", sql_strip):
            return jsonify({"error": "Only SELECT/CTE queries are allowed"}), 400
        sql_exec = SQLRewriter.rewrite_for_prefixes(sql_strip, prefixes) if prefixes else sql_strip

        # Validate
        try:
            with fa_engine.connect() as c:
                c.execute(text(f"EXPLAIN {sql_exec}"))
        except Exception as e:
            validation_info = {"error": f"validation failed: {e}"}
            sql_exec = None  # treat as not valid

    # If SQL missing or invalid, try to derive from admin_reply
    if not sql_exec and admin_reply:
        derived_sql, info = derive_sql_from_admin_reply(pipeline, inq, admin_reply)
        if derived_sql:
            sql_exec = derived_sql
        else:
            # Could not derive a runnable query → keep awaiting_admin and re-email admins
            # Persist admin note only; do NOT mark answered
            from core.inquiries import mark_admin_note
            try:
                mark_admin_note(mem, inquiry_id=inq_id, admin_reply=admin_reply, answered_by=answered_by)
            except Exception:
                pass

            # Notify admins for clearer guidance
            s = current_app.config["SETTINGS"]
            admin_list = s.get("ALERTS_EMAILS") or s.get("ADMIN_EMAILS") or []
            if isinstance(admin_list, str):
                admin_list = [x.strip() for x in admin_list.split(",") if x.strip()]

            try:
                body = (
                    f"Could not derive runnable SQL from admin reply.\n\n"
                    f"Inquiry #{inq_id}\n"
                    f"Question: {inq.get('question')}\n"
                    f"Admin reply: {admin_reply}\n\n"
                    f"Status from planner: {info.get('status') or info.get('error')}\n"
                    f"Follow-up questions: {', '.join(info.get('questions') or []) or '(none)'}\n"
                    f"Please reply with a SELECT/CTE SQL or clarify further via:\n"
                    f"POST /admin/inquiries/{inq_id}/reply\n"
                )
                if admin_list:
                    notify_admins_via_email(
                        subject=f"[Copilot] Still needs clarification — Inquiry #{inq_id}",
                        body_text=body,
                        to_emails=list(set(admin_list + ([answered_by] if answered_by else [])))
                    )
            except Exception:
                pass

            return jsonify({
                "ok": False,
                "inquiry_id": inq_id,
                "status": "awaiting_admin",
                "message": "Could not derive a valid query from admin_reply; emailed admins for more details",
                "details": info or validation_info
            }), 200

    if not sql_exec:
        # No valid query at all
        return jsonify({
            "ok": False,
            "inquiry_id": inq_id,
            "status": "awaiting_admin",
            "message": "No valid SQL to execute"
        }), 200

    # Execute + CSV
    try:
        with fa_engine.connect() as c:
            rs = c.execute(text(sql_exec))
            cols = list(rs.keys())
            sio = StringIO()
            w = csv.writer(sio)
            w.writerow(cols)
            for row in rs:
                w.writerow([row[c] for c in cols])
            csv_bytes = sio.getvalue().encode("utf-8")
    except Exception as e:
        return jsonify({"error": f"execution failed: {e}"}), 400

    # Email CSV to requester (no SQL in email)
    s = current_app.config["SETTINGS"]
    email_warning = None
    try:
        send_email_with_attachments(
            smtp_host=s.get("SMTP_HOST", "localhost"),
            smtp_port=int(s.get("SMTP_PORT", "465") or 465),
            smtp_user=s.get("SMTP_USER"),
            smtp_password=s.get("SMTP_PASSWORD"),
            mail_from=s.get("SMTP_FROM", "no-reply@example.com"),
            to=[auth_email] if auth_email else [],
            subject="[Copilot] Your data export is ready",
            body_text=(admin_reply or "Here is your requested data."),
            attachments=[("result.csv", csv_bytes, "text/csv")]
        )
    except Exception as e:
        email_warning = str(e)

    # Persist answer status
    from core.inquiries import mark_answered
    mark_answered(mem, inquiry_id=inq_id, answered_by=answered_by, admin_reply=admin_reply)

    # Optional: stash the provided/derived SQL for future reuse (not auto-verified yet)
    try:
        with mem.begin() as con:
            con.execute(text("""
                    INSERT INTO mem_snippets(namespace, title, description, sql_template, sql_raw, is_verified, verified_by, created_at, updated_at)
                    VALUES (:ns, :title, :desc, :tmpl, :raw, false, :by, NOW(), NOW())
                """), {
                "ns": namespace,
                "title": "Admin reply export",
                "desc": admin_reply or "Admin-provided query",
                "tmpl": (sql_in or "").strip() or "(derived from admin_reply)",
                "raw": sql_exec,
                "by": answered_by
            })
    except Exception:
        pass

    resp = {"ok": True, "inquiry_id": inq_id, "emailed_to": auth_email, "status": "answered"}
    if email_warning:
        resp["email_warning"] = email_warning
    return jsonify(resp)



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



