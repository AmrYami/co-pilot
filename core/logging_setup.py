from __future__ import annotations
import json
import logging
from typing import Any, Optional

from core.logging_utils import log_event, setup_logging


def init_logging(app=None, settings: Optional[Any] = None) -> None:
    """Ensure the root logger is configured and Flask logs propagate once."""

    setup_logging(settings)

    if app is not None:
        try:
            app.logger.handlers.clear()
            app.logger.setLevel(logging.getLogger().level)
            app.logger.propagate = True
        except Exception:  # pragma: no cover - defensive
            pass


def log_kv(logger, tag, payload) -> None:
    data = payload if isinstance(payload, dict) else {"value": payload}
    try:
        # Preserve legacy behaviour but route through structured logging
        log_event(logger, "kv", tag, json.loads(json.dumps(data, default=str)))
    except Exception:  # pragma: no cover - defensive
        logger.info("%s: %r", tag, payload)
