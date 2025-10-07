# -*- coding: utf-8 -*-
"""
Generic equality parser for phrases like:
- COLUMN = VALUE
- COLUMN equals VALUE
- COLUMN is VALUE

Maps display names (with spaces) to actual columns using DW_EXPLICIT_FILTER_COLUMNS.
Respects DW_ENUM_SYNONYMS for Contract.REQUEST_TYPE.
"""
from __future__ import annotations

import re
from typing import List, Dict


def parse_eq_pairs(text: str, explicit_cols: List[str]) -> List[Dict]:
    # Capture `COLUMN = VALUE` or `COLUMN is VALUE` or `COLUMN equals VALUE`
    # COLUMN may contain spaces. VALUE may be quoted or not.
    pat = r"(?P<col>[A-Za-z0-9_ ]+?)\s*(?:=|is|equals)\s*['\"]?(?P<val>[^'\"\n\r]+)['\"]?"
    eqs = []
    allowed = {c.strip().upper().replace(" ", "_") for c in explicit_cols or [] if isinstance(c, str)}
    for m in re.finditer(pat, text or "", flags=re.IGNORECASE):
        col = m.group("col").strip().upper().replace(" ", "_")
        val = m.group("val").strip()
        if col in allowed:
            eqs.append({"col": col, "val": val, "ci": True, "trim": True})
    return eqs


def expand_request_type_with_synonyms(eqs: List[Dict], enum_syn: Dict) -> List[Dict]:
    # If col == REQUEST_TYPE → expand synonyms into LIKE/IN plan handled later in SQL builder.
    # Here we just tag the record.
    for e in eqs:
        if str(e.get("col", "")).upper() == "REQUEST_TYPE":
            e["_use_synonyms"] = True
    return eqs
