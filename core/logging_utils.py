from __future__ import annotations

import logging
import logging.handlers
import os
import sys
from datetime import datetime
from typing import Any, Dict, Optional


def _log_dir(settings: Optional[Any]) -> str:
    """Resolve the directory used for log files."""

    try:
        value = (settings.get("LOG_DIR") if settings else None) or os.getenv("LOG_DIR")
    except Exception:
        value = os.getenv("LOG_DIR")
    return value or "logs"


def _log_level(settings: Optional[Any]) -> str:
    """Resolve the logging level from settings or environment."""

    try:
        value = (settings.get("LOG_LEVEL") if settings else None) or os.getenv("LOG_LEVEL")
    except Exception:
        value = os.getenv("LOG_LEVEL")
    return (value or "INFO").upper()


def _make_formatter() -> logging.Formatter:
    fmt = "%(asctime)s %(levelname)s [%(name)s]%(channel_tag)s %(message)s"

    class _Formatter(logging.Formatter):
        def format(self, record: logging.LogRecord) -> str:  # pragma: no cover - exercised indirectly
            channel = getattr(record, "channel", "")
            record.channel_tag = f" [{channel}]" if channel else ""
            return super().format(record)

    return _Formatter(fmt)


def setup_logging(settings: Optional[Any] = None) -> None:
    """Configure the root logger with stdout and a date-stamped file once."""

    root = logging.getLogger()
    if getattr(root, "_configured", False):
        return

    level_name = _log_level(settings)
    level = getattr(logging, level_name, logging.INFO)
    log_dir = _log_dir(settings)
    os.makedirs(log_dir, exist_ok=True)
    date_str = datetime.now().strftime("%Y-%m-%d")
    file_path = os.path.join(log_dir, f"log-{date_str}.log")

    for handler in list(root.handlers):
        root.removeHandler(handler)

    root.setLevel(level)

    console = logging.StreamHandler(sys.stdout)
    console.setLevel(level)
    console.setFormatter(_make_formatter())
    root.addHandler(console)

    file_handler = logging.FileHandler(file_path, encoding="utf-8")
    file_handler.setLevel(level)
    file_handler.setFormatter(_make_formatter())
    root.addHandler(file_handler)

    root._configured = True  # type: ignore[attr-defined]


def get_logger(name: str) -> logging.Logger:
    """Return a child logger that uses the configured root handlers."""

    setup_logging()
    logger = logging.getLogger(name)
    logger.propagate = True
    for handler in list(logger.handlers):
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

    import json

    data = payload or {}
    try:
        message = f"{event}: {json.dumps(data, ensure_ascii=False, default=str)}"
    except Exception:
        message = f"{event}: {data!r}"
    logger.log(level, message, extra={"channel": channel})


__all__ = ["setup_logging", "get_logger", "log_event"]
