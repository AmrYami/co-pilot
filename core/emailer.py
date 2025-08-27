"""
core/emailer.py
Lightweight SMTP helper that pulls config from Settings (DB-backed) with env fallback.
Supports SSL (465) and STARTTLS (587). Attachments optional.
"""
from __future__ import annotations
import smtplib, ssl, mimetypes, os
from email.message import EmailMessage
from typing import Any, Iterable, Optional, Tuple

from core.settings import Settings

class Emailer:
    def __init__(self, settings: Settings) -> None:
        self.s = settings

    def _smtp_config(self) -> Tuple[str, int, str | None, str | None, str]:
        host = self.s.get("SMTP_HOST", "localhost")
        port = int(self.s.get("SMTP_PORT", 25))
        user = self.s.get("SMTP_USER")  # can be None for unauthenticated
        pwd  = self.s.get("SMTP_PASSWORD")  # can be None
        from_addr = self.s.get("SMTP_FROM", "Copilot <no-reply@localhost>")
        return host, port, user, pwd, from_addr

    def send(
        self,
        to: Iterable[str],
        subject: str,
        html: str | None = None,
        text: str | None = None,
        cc: Iterable[str] | None = None,
        bcc: Iterable[str] | None = None,
        attachments: Optional[Iterable[Tuple[str, bytes, str | None]]] = None,
    ) -> dict[str, Any]:
        """
        Send an email.
        - `to`, `cc`, `bcc`: lists of recipients.
        - `attachments`: iterable of (filename, data_bytes, mime_type_or_None).
        Returns a small dict with 'ok' and 'recipients'.
        """
        host, port, user, pwd, from_addr = self._smtp_config()
        if not text and not html:
            text = "(no content)"

        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = from_addr
        msg["To"] = ", ".join(list(to))
        if cc:
            msg["Cc"] = ", ".join(list(cc))
        all_rcpts = list(to) + (list(cc) if cc else []) + (list(bcc) if bcc else [])

        if html and text:
            msg.set_content(text)
            msg.add_alternative(html, subtype="html")
        elif html:
            msg.add_alternative(html, subtype="html")
        else:
            msg.set_content(text or "")

        # Attachments
        if attachments:
            for fname, data, mtype in attachments:
                ctype = mtype or (mimetypes.guess_type(fname)[0] or "application/octet-stream")
                maintype, subtype = ctype.split("/", 1)
                msg.add_attachment(data, maintype=maintype, subtype=subtype, filename=fname)

        # SSL (465) vs STARTTLS (587)
        if port == 465:
            context = ssl.create_default_context()
            with smtplib.SMTP_SSL(host, port, context=context) as smtp:
                if user and pwd:
                    smtp.login(user, pwd)
                smtp.send_message(msg, to_addrs=all_rcpts)
        else:
            with smtplib.SMTP(host, port, timeout=30) as smtp:
                smtp.ehlo()
                try:
                    smtp.starttls(context=ssl.create_default_context())
                    smtp.ehlo()
                except smtplib.SMTPException:
                    # fine if the server doesn't support STARTTLS and you’re on 25
                    pass
                if user and pwd:
                    smtp.login(user, pwd)
                smtp.send_message(msg, to_addrs=all_rcpts)

        return {"ok": True, "recipients": all_rcpts}
