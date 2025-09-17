from __future__ import annotations

import re
from typing import Optional


_CODE_FENCE_START_RE = re.compile(r"^```[\w-]*\s*$")
_CODE_FENCE_END_RE = re.compile(r"^```\s*$")
_SQL_PREFIX_RE = re.compile(r"^\s*(SELECT|WITH)\b", re.IGNORECASE)


def strip_code_fences(text: Optional[str]) -> str:
    """Remove surrounding Markdown code fences from *text* if present."""
    if text is None:
        return ""
    lines = text.splitlines()
    if not lines:
        return ""

    start = 0
    end = len(lines)

    if _CODE_FENCE_START_RE.match(lines[0].strip()):
        start += 1
    if end > start and _CODE_FENCE_END_RE.match(lines[-1].strip()):
        end -= 1

    cleaned = "\n".join(lines[start:end]).strip()
    return cleaned or ""


def is_select_like(text: Optional[str]) -> bool:
    """Return True when *text* appears to start with a SQL SELECT or WITH."""
    if not text:
        return False
    return bool(_SQL_PREFIX_RE.match(text))
