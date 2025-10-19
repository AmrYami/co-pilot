from __future__ import annotations

import json
import logging
import os
import sys
import time
import uuid
from contextvars import ContextVar
from logging.handlers import RotatingFileHandler

_corr_id: ContextVar[str] = ContextVar("_corr_id", default=None)


def set_corr_id(value: str | None = None) -> str:
    cid = value or str(uuid.uuid4())
    _corr_id.set(cid)
    return cid


def get_corr_id() -> str | None:
    return _corr_id.get()


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
