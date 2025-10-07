# -*- coding: utf-8 -*-
"""
Order/Top/Bottom detectors.
"""
import re

ASC_TOKENS = {"lowest", "cheapest", "smallest", "bottom", "أقل"}
DESC_TOKENS = {"highest", "top", "biggest", "largest", "أعلى"}


def detect_order_direction(text: str, default_desc: bool = True) -> str:
    t = (text or "").lower()
    if any(tok in t for tok in ASC_TOKENS):
        return "ASC"
    if any(tok in t for tok in DESC_TOKENS):
        return "DESC"
    return "DESC" if default_desc else "ASC"


def detect_top_n(text: str) -> int | None:
    # e.g., "top 10", "bottom 5"
    m = re.search(r"\b(?:top|bottom)\s+(\d{1,3})\b", (text or "").lower())
    return int(m.group(1)) if m else None
