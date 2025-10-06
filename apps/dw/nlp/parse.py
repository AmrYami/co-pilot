"""Parsing helpers for DW natural language inputs."""

from __future__ import annotations

import re
from typing import List, Tuple

__all__ = ["normalize_question", "extract_equalities_first"]


def normalize_question(q: str) -> str:
    """Return a lightly normalized question string."""

    text = (q or "").strip()
    if not text:
        return ""
    text = re.sub(r"\s+", " ", text)
    return text


_EQ_PATTERN = re.compile(
    r"""(?ix)
    \b
    (?P<col>[A-Z0-9_ ]+?)
    \s*(?:=|==|is|equals)\s*
    (?:
        \"(?P<val_dq>[^\"]+)\" |
        '(?P<val_sq>[^']+)' |
        (?P<val_bare>[^,;\s]+)
    )
    """
)


def extract_equalities_first(q: str) -> Tuple[str, List[Tuple[str, str]]]:
    """Extract ``COLUMN = VALUE`` pairs and return cleaned text + matches."""

    matches: List[Tuple[str, str]] = []
    spans: List[Tuple[int, int]] = []
    for match in _EQ_PATTERN.finditer(q or ""):
        col = (match.group("col") or "").strip()
        val = (
            match.group("val_dq")
            or match.group("val_sq")
            or match.group("val_bare")
            or ""
        ).strip()
        if not col or not val:
            continue
        matches.append((col, val))
        spans.append(match.span())

    cleaned = q or ""
    for start, end in sorted(spans, key=lambda item: item[0], reverse=True):
        cleaned = cleaned[:start] + " " + cleaned[end:]

    return re.sub(r"\s+", " ", cleaned).strip(), matches
