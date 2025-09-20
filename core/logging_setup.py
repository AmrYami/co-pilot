import os
import json
import logging
from datetime import datetime

_FMT = "%(asctime)s %(levelname)s [%(name)s] %(message)s"


def init_logging(app=None):
    """Configure root + optional Flask app loggers."""
    level_env = os.getenv("COPILOT_DEBUG", "0")
    level = logging.DEBUG if level_env in ("1", "true", "True") else logging.INFO
    root = logging.getLogger()

    # Avoid duplicate handlers on reload
    for handler in list(root.handlers):
        root.removeHandler(handler)
    root.setLevel(level)

    formatter = logging.Formatter(_FMT)

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(level)
    stream_handler.setFormatter(formatter)
    root.addHandler(stream_handler)

    log_dir = os.getenv("COPILOT_LOG_DIR", "logs")
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(
        log_dir, f"log-{datetime.now().strftime('%Y-%m-%d')}.log"
    )
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)

    if app is not None:
        app.logger.handlers = []
        app.logger.setLevel(level)
        app.logger.propagate = True


def log_kv(logger, tag, payload):
    try:
        logger.info("%s: %s", tag, json.dumps(payload, default=str)[:4000])
    except Exception:
        logger.info("%s: %r", tag, payload)
