"""Lightweight in-memory store for recent /dw/rate patches."""

from __future__ import annotations

import re
import threading
import time
from typing import Any, Dict, List, Tuple


_LOCK = threading.RLock()
_PATCHES: Dict[str, List[Tuple[float, Dict[str, Any]]]] = {}
_MAX_PER_KEY = 5


def normalize_question(question: str) -> str:
    """Normalize a question for use as a dictionary key."""

    text = (question or "").strip().lower()
    text = re.sub(r"\s+", " ", text)
    return text


def _prune_locked(key: str, ttl_seconds: int) -> None:
    cutoff = time.time() - ttl_seconds
    entries = _PATCHES.get(key)
    if not entries:
        return
    _PATCHES[key] = [entry for entry in entries if entry[0] >= cutoff]
    if len(_PATCHES[key]) > _MAX_PER_KEY:
        _PATCHES[key] = _PATCHES[key][-_MAX_PER_KEY:]


def store_rate_hints(question: str, hints: Dict[str, Any], *, ttl_seconds: int = 900) -> None:
    """Store parsed rate hints for a normalized question with a TTL."""

    key = normalize_question(question)
    if not key or not hints:
        return
    snapshot = dict(hints)
    now = time.time()
    with _LOCK:
        bucket = _PATCHES.setdefault(key, [])
        bucket.append((now, snapshot))
        _prune_locked(key, ttl_seconds)


def load_recent_hints(question: str, ttl_seconds: int = 900) -> List[Dict[str, Any]]:
    """Return recent hint dictionaries for the given question."""

    key = normalize_question(question)
    if not key:
        return []
    with _LOCK:
        _prune_locked(key, ttl_seconds)
        entries = list(_PATCHES.get(key, []))
    return [dict(item[1]) for item in entries]


__all__ = [
    "normalize_question",
    "store_rate_hints",
    "load_recent_hints",
]
