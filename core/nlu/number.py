"""Utilities for extracting numeric hints from text."""

from __future__ import annotations

import re
from typing import Optional

try:  # pragma: no cover - optional dependency in test envs
    from word2number import w2n
except Exception:  # pragma: no cover - graceful degradation
    w2n = None  # type: ignore

def _word_to_num(token: str) -> Optional[int]:
    if not token:
        return None
    if token.isdigit():
        return int(token)
    if w2n is None:
        return None
    try:
        return w2n.word_to_num(token)
    except Exception:
        return None


def extract_top_n(text: str) -> int | None:
    """Return the requested Top-N value if present."""

    t = (text or "").strip().lower()
    if not t:
        return None

    match = re.search(r"\btop\s+(\d+)\b", t)
    if match:
        return int(match.group(1))

    if "top " in t and w2n is not None:
        rest = t.split("top ", 1)[1].split()
        if rest:
            value = _word_to_num(rest[0])
            if value is not None:
                return value

    return None
