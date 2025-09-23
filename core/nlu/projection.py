from __future__ import annotations

import re
from typing import Dict, List


def extract_projection(question: str, mapping: Dict[str, str]) -> List[str]:
    """
    Heuristically extract an ordered list of requested columns from a question.
    Looks for parentheses "(a, b, c)" and phrases like "columns/fields/show/select a, b".
    Returns uppercase column names using `mapping` (unknown tokens are ignored).
    """
    q = (question or "").strip()
    wanted: List[str] = []
    parts: List[str] = []

    # (1) Parentheses e.g. "... (contract id, owner, request date)"
    m = re.search(r"\(([^()]+)\)", q)
    if m:
        parts += re.split(r"[,\|/]+", m.group(1))

    # (2) After cue words
    cue = re.search(r"\b(columns?|fields?|include|show|select)\b[:\- ]+(.+)$", q, re.I)
    if cue:
        parts += re.split(r"[,\|/]+", cue.group(2))

    for p in parts:
        t = re.sub(r"\s+", " ", p.strip().lower())
        if not t:
            continue
        col = mapping.get(t)
        if col:
            wanted.append(col)

    # Deduplicate preserving order
    seen, ordered = set(), []
    for c in wanted:
        if c not in seen:
            seen.add(c)
            ordered.append(c)
    return ordered
