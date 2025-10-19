from __future__ import annotations

import json
import logging
import os
import sys
import time
from logging.handlers import RotatingFileHandler

from core.corr import get_corr_id as corr_get_corr_id, set_corr_id as corr_set_corr_id


def set_corr_id(value: str | None = None) -> str:
    """Backward compatible proxy to :func:`core.corr.set_corr_id`."""

    return corr_set_corr_id(value)


def get_corr_id() -> str | None:
    """Backward compatible proxy to :func:`core.corr.get_corr_id`."""

    return corr_get_corr_id()


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        base = {
            "ts": int(time.time() * 1000),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
            "corr_id": get_corr_id(),
        }
        if isinstance(record.args, dict):
            base.update(record.args)
        return json.dumps(base, ensure_ascii=False)


def setup_logging(debug: bool = False):
    root = logging.getLogger()
    root.setLevel(logging.DEBUG if debug else logging.INFO)

    # Console JSON
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(JsonFormatter())
    root.handlers[:] = [sh]

    # Optional file logs (export LOG_FILE=/var/log/copilot.log)
    log_file = os.getenv("LOG_FILE")
    if log_file:
        fh = RotatingFileHandler(log_file, maxBytes=10_000_000, backupCount=3)
        fh.setFormatter(JsonFormatter())
        root.addHandler(fh)

    # Reduce noise
    logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)
    logging.getLogger("werkzeug").setLevel(logging.INFO)

    # App loggers
    logging.getLogger("dw").setLevel(logging.DEBUG if debug else logging.INFO)
    logging.getLogger("apps.dw").setLevel(logging.DEBUG if debug else logging.INFO)
