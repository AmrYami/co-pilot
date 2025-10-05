# -*- coding: utf-8 -*-
"""Parse /dw/rate free-text comments into structured hints."""
from __future__ import annotations

import re
from typing import Dict, List, Optional

_WS = r"\s*"


def _split_kv_parts(comment: str) -> List[str]:
    """Split the comment by ';' while removing empty parts."""
    if not comment:
        return []
    return [part.strip() for part in re.split(r";", comment) if part.strip()]


def _normalize(value: Optional[str]) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", value).strip()


def parse_rate_comment(comment: str) -> Dict[str, object]:
    """Return structured hints extracted from the ``comment`` text.

    The returned dictionary contains the following keys:

    ``fts_tokens``
        List of tokens extracted from ``fts:`` parts.

    ``fts_operator``
        Either ``"AND"`` or ``"OR"`` depending on the separators used.

    ``eq_filters``
        List of dictionaries with ``col``/``val`` pairs coming from ``eq:`` hints.

    ``group_by``
        Optional column passed via ``group_by:`` hint.

    ``gross``
        Optional boolean value parsed from ``gross:`` hint.

    ``sort_by`` / ``sort_desc``
        Column name and direction parsed from ``order_by:`` hint.
    """
    out: Dict[str, object] = {
        "fts_tokens": [],
        "fts_operator": "OR",
        "eq_filters": [],
        "group_by": None,
        "gross": None,
        "sort_by": None,
        "sort_desc": None,
    }
    if not comment:
        return out

    parts = _split_kv_parts(comment)
    for part in parts:
        # fts: it | home care   OR   fts: it & home care
        match = re.match(r"^\s*fts\s*:\s*(.+)$", part, flags=re.IGNORECASE)
        if match:
            body = match.group(1).strip()
            if "&" in body or re.search(r"\band\b", body, flags=re.IGNORECASE):
                tokens = [tok.strip() for tok in re.split(r"[&]|(?i:\band\b)", body) if tok.strip()]
                out["fts_operator"] = "AND"
            else:
                tokens = [tok.strip() for tok in body.split("|") if tok.strip()]
                out["fts_operator"] = "OR"
            out["fts_tokens"] = [_normalize(tok) for tok in tokens]
            continue

        match = re.match(r"^\s*eq\s*:\s*([A-Za-z0-9_]+)\s*=\s*(.+)$", part, flags=re.IGNORECASE)
        if match:
            column = _normalize(match.group(1).upper())
            value = _normalize(match.group(2))
            out.setdefault("eq_filters", []).append(
                {"col": column, "val": value, "ci": True, "trim": True}
            )
            continue

        match = re.match(r"^\s*group_by\s*:\s*([A-Za-z0-9_]+)\s*$", part, flags=re.IGNORECASE)
        if match:
            out["group_by"] = _normalize(match.group(1).upper())
            continue

        match = re.match(r"^\s*gross\s*:\s*(true|false)\s*$", part, flags=re.IGNORECASE)
        if match:
            out["gross"] = match.group(1).lower() == "true"
            continue

        match = re.match(
            r"^\s*order_by\s*:\s*([A-Za-z0-9_]+)\s*(asc|desc)?\s*$",
            part,
            flags=re.IGNORECASE,
        )
        if match:
            out["sort_by"] = _normalize(match.group(1).upper())
            direction = match.group(2).lower() if match.group(2) else "desc"
            out["sort_desc"] = direction == "desc"
            continue

    return out


__all__ = ["parse_rate_comment"]
