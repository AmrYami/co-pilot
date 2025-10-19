from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, Optional

from core.logging_setup import setup_logging as _setup_structured_logging


def setup_logging(settings: Optional[Any] = None) -> None:
    """Configure structured logging using environment-aware debug flag."""

    debug_env = os.getenv("COPILOT_DEBUG", "0")
    debug = str(debug_env).lower() in {"1", "true", "yes", "on"}
    _setup_structured_logging(debug=debug, preserve_handlers=True)


def get_logger(name: str) -> logging.Logger:
    """Return a logger that propagates to the structured root handlers."""

    setup_logging()
    logger = logging.getLogger(name)
    logger.propagate = True
    for handler in list(getattr(logger, "handlers", [])):
        logger.removeHandler(handler)
    return logger


def log_event(
    logger: logging.Logger,
    channel: str,
    event: str,
    payload: Dict[str, Any] | None = None,
    *,
    level: int = logging.INFO,
) -> None:
    """Log a structured event with a consistent JSON payload."""

    data = payload or {}
    try:
        message = json.dumps({"event": event, **data}, ensure_ascii=False, default=str)
    except Exception:
        message = f"{event}: {data!r}"
    logger.log(level, message, extra={"channel": channel})


__all__ = ["setup_logging", "get_logger", "log_event"]
