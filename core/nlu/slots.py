"""Slot extraction helpers for deterministic NLU."""

from __future__ import annotations

import re

_DIMENSION_MAP = {
    "owner department": "OWNER_DEPARTMENT",
    "department": "OWNER_DEPARTMENT",
    "entity": "ENTITY_NO",
    "owner": "CONTRACT_OWNER",
    "stakeholder": "CONTRACT_STAKEHOLDER_1",
}


def extract_group_by(text: str) -> str | None:
    t = (text or "").lower()
    match = re.search(r"\b(?:by|per)\s+([a-z_ ]+)\b", t)
    if not match:
        return None
    key = match.group(1).strip()
    for alias, column in _DIMENSION_MAP.items():
        if alias in key:
            return column
    return None


def wants_gross(text: str) -> bool:
    t = (text or "").lower()
    return "gross" in t


def wants_count(text: str) -> bool:
    t = (text or "").lower()
    return " count" in t or "(count)" in t
