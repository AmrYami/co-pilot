# -*- coding: utf-8 -*-
"""
Parse /dw/rate comment hints like:
- fts: it | home care
- fts: it & home care
- eq: ENTITY = DSFH
- group_by: CONTRACT_STATUS
- gross: true
- order_by: REQUEST_DATE desc
"""
from __future__ import annotations

import re
from typing import Dict


def parse_rate_comment(comment: str) -> Dict[str, object]:
    out = {"fts": None, "eq": [], "group_by": None, "gross": None, "order_by": None}
    if not comment:
        return out
    lines = [x.strip() for x in comment.split(";") if x.strip()]
    for ln in lines:
        if ln.lower().startswith("fts:"):
            payload = ln.split(":", 1)[1].strip()
            # support OR with |, AND with &
            if "&" in payload and "|" not in payload:
                tokens = [t.strip() for t in payload.split("&") if t.strip()]
                out["fts"] = {"mode": "AND", "tokens": tokens}
            else:
                tokens = [t.strip() for t in re.split(r"[|,]", payload) if t.strip()]
                out["fts"] = {"mode": "OR", "tokens": tokens}
        elif ln.lower().startswith("eq:"):
            payload = ln.split(":", 1)[1].strip()
            m = re.match(r"([A-Za-z0-9_ ]+)\s*=\s*(.+)$", payload)
            if m:
                col = m.group(1).strip().upper().replace(" ", "_")
                val = m.group(2).strip().strip("'").strip('"')
                out["eq"].append({"col": col, "val": val, "ci": True, "trim": True})
        elif ln.lower().startswith("group_by:"):
            out["group_by"] = ln.split(":", 1)[1].strip().upper().replace(" ", "_")
        elif ln.lower().startswith("gross:"):
            raw = ln.split(":", 1)[1].strip().lower()
            out["gross"] = (raw in ("1", "true", "yes", "on"))
        elif ln.lower().startswith("order_by:"):
            part = ln.split(":", 1)[1].strip()
            m = re.match(r"([A-Za-z0-9_ ]+)\s+(asc|desc)$", part, flags=re.IGNORECASE)
            if m:
                out["order_by"] = {"col": m.group(1).strip().upper().replace(" ", "_"), "dir": m.group(2).upper()}
    return out
