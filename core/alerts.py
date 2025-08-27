# core/alerts.py
from __future__ import annotations
from typing import Iterable, Optional
from sqlalchemy import text
from sqlalchemy.engine import Engine
from flask import current_app

from core.mailer import send_email_with_attachments  # use new emailer

def queue_alert(mem_engine: Engine, *, namespace: str, event_type: str, payload: dict, recipient: Optional[str]=None) -> int:
    """
    Store an alert in mem_alerts (status=queued). Returns alert id.
    """
    sql = text("""
        INSERT INTO mem_alerts(namespace, event_type, recipient, payload, status, created_at)
        VALUES (:ns, :et, :rcpt, :payload, 'queued', NOW())
        RETURNING id
    """)
    with mem_engine.begin() as con:
        alert_id = con.execute(sql, {
            "ns": namespace,
            "et": event_type,
            "rcpt": recipient,
            "payload": payload,
        }).scalar_one()
    return int(alert_id)

def notify_admins_via_email(*, subject: str, body_text: str, to_emails: Iterable[str]) -> None:
    s = current_app.config["SETTINGS"]
    from core.mailer import send_email
    send_email(
        smtp_host=s.get("SMTP_HOST", "localhost"),
        smtp_port=int(s.get("SMTP_PORT", "465") or 465),
        smtp_user=s.get("SMTP_USER"),
        smtp_password=s.get("SMTP_PASSWORD"),
        mail_from=s.get("SMTP_FROM", "no-reply@example.com"),
        to=list(to_emails),
        subject=subject,
        body_text=body_text,
        smtp_security=s.get("SMTP_SECURITY"),  # <- new, optional
    )
