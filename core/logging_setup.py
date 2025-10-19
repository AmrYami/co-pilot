from __future__ import annotations

import json
import logging
import sys
import time

from core.corr import get_corr_id as corr_get_corr_id, set_corr_id as corr_set_corr_id


def set_corr_id(value: str | None = None) -> str:
    """Backward compatible proxy to :func:`core.corr.set_corr_id`."""

    return corr_set_corr_id(value)


def get_corr_id() -> str | None:
    """Backward compatible proxy to :func:`core.corr.get_corr_id`."""

    return corr_get_corr_id()


class JsonFormatter(logging.Formatter):
    """Format log records as structured JSON with correlation identifiers."""

    def format(self, record: logging.LogRecord) -> str:
        base: dict[str, object] = {
            "ts": int(time.time() * 1000),
            "level": record.levelname,
            "logger": record.name,
            "corr_id": get_corr_id(),
        }

        channel = getattr(record, "channel", None)
        if channel:
            base["channel"] = channel

        if isinstance(record.msg, dict):
            base.update(record.msg)
        else:
            base["message"] = record.getMessage()

        if record.exc_info:
            base["exc_info"] = self.formatException(record.exc_info)
        if record.stack_info:
            base["stack"] = record.stack_info

        return json.dumps(base, ensure_ascii=False, default=str)


def setup_logging(debug: bool = False, *, preserve_handlers: bool = True) -> None:
    """Configure root logging while leaving existing handlers untouched."""

    root = logging.getLogger()

    if not preserve_handlers and root.handlers:
        for handler in list(root.handlers):
            root.removeHandler(handler)

    if not root.handlers:
        stream = logging.StreamHandler(sys.stdout)
        stream.setFormatter(JsonFormatter())
        root.addHandler(stream)

    root.setLevel(logging.DEBUG if debug else logging.INFO)

    logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)
    logging.getLogger("werkzeug").setLevel(logging.INFO)

    for name in ("dw", "apps.dw", "apps.dw.admin_api", "apps.dw.routes"):
        logger = logging.getLogger(name)
        logger.setLevel(logging.DEBUG if debug else logging.INFO)
        logger.propagate = True

