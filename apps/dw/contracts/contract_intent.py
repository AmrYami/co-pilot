# -*- coding: utf-8 -*-
"""
Contract intent parsing helpers.
All comments and strings inside code are in English only.
"""

import re
from typing import Dict, Any, Optional

REQUEST_TYPE_PAT = re.compile(r"\brequest[_\s]*type\s*(=|is|equals)\s*(['\"]?)([A-Za-z\-\s]+)\2", re.IGNORECASE)


def parse_request_type_filter(q: str) -> Optional[str]:
    """
    Extract the requested REQUEST_TYPE value from the NL question.
    Returns normalized (as typed) string, or None.
    """
    m = REQUEST_TYPE_PAT.search(q or "")
    if not m:
        return None
    val = (m.group(3) or "").strip()
    return val if val else None


def _ensure_filter_list(container: Any) -> list:
    if isinstance(container, dict):
        filters = container.setdefault("filters", [])
        if isinstance(filters, list):
            return filters
        new_list: list = []
        container["filters"] = new_list
        return new_list
    # Dataclass or object with attribute
    filters = getattr(container, "filters", None)
    if isinstance(filters, list):
        return filters
    new_list = []
    setattr(container, "filters", new_list)
    return new_list


def apply_contract_filters_from_text(intent: Dict[str, Any]) -> None:
    """
    If the user explicitly mentions a Contract column with equality,
    attach a structured filter to the intent.
    """
    notes = intent.get("notes") if isinstance(intent, dict) else getattr(intent, "notes", {})
    q_text = (notes or {}).get("q") or getattr(intent, "raw_q", "") or getattr(intent, "question", "")
    if not q_text and isinstance(intent, dict):
        q_text = intent.get("raw_q") or intent.get("question") or intent.get("q", "")
    q = q_text or ""
    val = parse_request_type_filter(q)
    if val:
        filters = _ensure_filter_list(intent)
        filters.append({"column": "REQUEST_TYPE", "kind": "enum", "value": val})
