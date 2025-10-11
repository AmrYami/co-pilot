"""Lightweight parser for rate comments used by simplified DW flow."""

from __future__ import annotations

from typing import Dict, List


def parse_rate_comment(comment: str) -> Dict[str, object]:
    """Parse ``comment`` into an intent dictionary for the simple DW builder."""

    intent: Dict[str, object] = {
        "fts_groups": [],
        "fts_operator": "OR",
        "boolean_groups": [{"id": "A", "fields": []}],
        "sort_by": "REQUEST_DATE",
        "sort_desc": True,
        "eq_filters": [],
    }

    if not comment:
        return intent

    parts = [part.strip() for part in comment.split(";") if part.strip()]
    for part in parts:
        lower = part.lower()
        if lower.startswith("fts:"):
            payload = part.split(":", 1)[1]
            tokens = [tok.strip() for tok in payload.split("or") if tok.strip()]
            if tokens:
                intent["fts_groups"] = [[tok] for tok in tokens]
        elif lower.startswith("eq:"):
            body = part.split(":", 1)[1].strip()
            if "=" not in body:
                continue
            field, values_str = body.split("=", 1)
            field = field.strip()
            values = [value.strip() for value in values_str.split("or") if value.strip()]
            if values:
                intent["boolean_groups"][0]["fields"].append(
                    {"field": field, "op": "eq", "values": values}
                )
        elif lower.startswith("order_by:"):
            payload = part.split(":", 1)[1].strip()
            if not payload:
                continue
            bits = payload.split()
            sort_by = bits[0].strip() if bits else ""
            if sort_by:
                intent["sort_by"] = sort_by.replace("_DESC", "")
            if len(bits) > 1:
                intent["sort_desc"] = bits[1].lower().startswith("desc")
        elif lower.startswith("fts_operator:"):
            value = part.split(":", 1)[1].strip().upper()
            if value in {"AND", "OR"}:
                intent["fts_operator"] = value

    intent["eq_filters"] = []
    return intent


__all__ = ["parse_rate_comment"]
