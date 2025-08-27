# core/mailer.py
"""
Minimal SMTP mailer with attachment support.
Settings keys expected:
  SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASSWORD, SMTP_FROM, SMTP_SECURITY
Where SMTP_SECURITY in {"ssl","starttls","plain"}; if omitted it's inferred:
  port 465 -> ssl, port 587 -> starttls, else plain
"""

from __future__ import annotations
import smtplib, ssl
from email.message import EmailMessage
from typing import Iterable, List, Tuple, Optional

def _infer_security(port: int, configured: Optional[str]) -> str:
    sec = (configured or "").strip().lower()
    if sec in {"ssl", "starttls", "plain"}:
        return sec
    if port == 465:
        return "ssl"
    if port == 587:
        return "starttls"
    return "plain"

def _smtp_connect(host: str, port: int, security: str):
    """Create and return an smtplib client connected & TLS-negotiated per security mode."""
    security = _infer_security(port, security)
    ctx = ssl.create_default_context()
    if security == "ssl":
        return smtplib.SMTP_SSL(host, port, context=ctx, timeout=30)
    client = smtplib.SMTP(host, port, timeout=30)
    if security == "starttls":
        client.ehlo()
        client.starttls(context=ctx)
        client.ehlo()
    # if "plain": nothing extra
    return client

def _send(msg: EmailMessage, *, smtp_host: str, smtp_port: int,
          smtp_user: str | None, smtp_password: str | None,
          smtp_security: Optional[str]) -> None:
    """
    Open connection (with STARTTLS/SSL/plain), login if creds provided, send, close.
    Includes automatic fallback from SSL→STARTTLS on WRONG_VERSION_NUMBER.
    """
    try:
        client = _smtp_connect(smtp_host, smtp_port, smtp_security)
    except ssl.SSLError as e:
        # Common mismatch: SSL to a STARTTLS server (e.g., port 587)
        if "WRONG_VERSION_NUMBER" in str(e):
            client = _smtp_connect(smtp_host, smtp_port, "starttls")
        else:
            raise
    try:
        if smtp_user and smtp_password:
            client.login(smtp_user, smtp_password)
        client.send_message(msg)
    finally:
        try:
            client.quit()
        except Exception:
            pass

def send_email(
    *,
    smtp_host: str,
    smtp_port: int,
    smtp_user: str | None,
    smtp_password: str | None,
    mail_from: str,
    to: Iterable[str],
    subject: str,
    body_text: str,
    smtp_security: Optional[str] = None
) -> None:
    msg = EmailMessage()
    msg["From"] = mail_from
    msg["To"] = ", ".join(list(to))
    msg["Subject"] = subject
    msg.set_content(body_text)
    _send(
        msg,
        smtp_host=smtp_host,
        smtp_port=smtp_port,
        smtp_user=smtp_user,
        smtp_password=smtp_password,
        smtp_security=smtp_security,
    )

def send_email_with_attachments(
    *,
    smtp_host: str,
    smtp_port: int,
    smtp_user: str | None,
    smtp_password: str | None,
    mail_from: str,
    to: List[str],
    subject: str,
    body_text: str,
    attachments: List[Tuple[str, bytes, str]],
    smtp_security: Optional[str] = None
) -> None:
    msg = EmailMessage()
    msg["From"] = mail_from
    msg["To"] = ", ".join(to)
    msg["Subject"] = subject
    msg.set_content(body_text)

    for fname, data, mime in attachments:
        maintype, subtype = (mime.split("/", 1) + ["octet-stream"])[:2]
        msg.add_attachment(data, maintype=maintype, subtype=subtype, filename=fname)

    _send(
        msg,
        smtp_host=smtp_host,
        smtp_port=smtp_port,
        smtp_user=smtp_user,
        smtp_password=smtp_password,
        smtp_security=smtp_security,
    )
