from __future__ import annotations

import logging
import os
import sys
from datetime import datetime

_FMT = "%(asctime)s %(levelname)s [%(name)s] %(message)s"


def _env_truthy(value: str | None, *, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _resolve_level() -> int:
    level_name = os.getenv("LOG_LEVEL")
    if level_name:
        return getattr(logging, level_name.upper(), logging.INFO)
    debug_env = os.getenv("COPILOT_DEBUG")
    return logging.DEBUG if _env_truthy(debug_env) else logging.INFO


def setup_root_logging() -> None:
    """Configure the root logger once, guarding against duplicate handlers."""

    level = _resolve_level()
    root = logging.getLogger()

    if root.handlers:
        root.setLevel(level)
        return

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(level)
    stream_handler.setFormatter(logging.Formatter(_FMT))
    root.addHandler(stream_handler)

    log_dir = os.getenv("COPILOT_LOG_DIR")
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)
        log_path = os.path.join(
            log_dir, f"log-{datetime.now().strftime('%Y-%m-%d')}.log"
        )
        file_handler = logging.FileHandler(log_path, encoding="utf-8")
        file_handler.setLevel(level)
        file_handler.setFormatter(logging.Formatter(_FMT))
        root.addHandler(file_handler)

    root.setLevel(level)


def get_logger(name: str) -> logging.Logger:
    """Return a child logger that relies on the root handler."""

    setup_root_logging()
    logger = logging.getLogger(name)
    logger.propagate = True
    if logger.handlers:
        for handler in list(logger.handlers):
            logger.removeHandler(handler)
    return logger
