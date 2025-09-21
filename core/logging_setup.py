import json
import logging

from core.logging_utils import setup_root_logging


def init_logging(app=None):
    """Ensure the root logger is configured and Flask logs propagate once."""

    setup_root_logging()

    if app is not None:
        app.logger.handlers = []
        app.logger.setLevel(logging.getLogger().level)
        app.logger.propagate = True


def log_kv(logger, tag, payload):
    try:
        logger.info("%s: %s", tag, json.dumps(payload, default=str)[:4000])
    except Exception:
        logger.info("%s: %r", tag, payload)
