import json
from typing import Dict, List, Tuple, Any
from apps.dw.lib.sql_utils import is_email, is_phone
import hashlib

VALUE_TEXT = "TEXT"
VALUE_EMAIL = "EMAIL"
VALUE_PHONE = "PHONE"
VALUE_NUMBER = "NUMBER"


def _value_type(v: str) -> str:
    s = (v or "").strip()
    if is_email(s):
        return VALUE_EMAIL
    if is_phone(s):
        return VALUE_PHONE
    try:
        float(s)
        return VALUE_NUMBER
    except Exception:
        return VALUE_TEXT


def build_intent_signature(intent: Dict[str, Any]) -> Dict[str, Any]:
    """
    Value-agnostic signature capturing shapes and types only.
    eq becomes {COL: {op: in|eq, types: [EMAIL|PHONE|NUMBER|TEXT]}}
    fts drops emails/phones and lowercases tokens.
    """
    sig: Dict[str, Any] = {"eq": {}, "fts": [], "group_by": [], "order": {}}
    for col, values in intent.get("eq_filters", []) or []:
        col_u = str(col or "").upper()
        types = sorted({_value_type(v) for v in (values or [])})
        sig["eq"][col_u] = {"op": "in" if len(values or []) > 1 else "eq", "types": types}

    cross = intent.get("or_groups") or []
    if cross:
        sig["or_groups"] = []
        for grp in cross:
            cols = sorted({str(c or "").upper() for c, _ in (grp or [])})
            if cols:
                sig["or_groups"].append(cols)
        sig["or_groups"] = sorted(sig["or_groups"]) if sig.get("or_groups") else []

    toks: List[str] = []
    for g in intent.get("fts_groups", []) or []:
        for t in g or []:
            t0 = (t or "").strip()
            if not t0 or is_email(t0) or is_phone(t0):
                continue
            toks.append(t0.lower())
    if toks:
        sig["fts"] = sorted(list(dict.fromkeys(toks)))

    if intent.get("group_by"):
        sig["group_by"] = [str(c or "").upper() for c in intent.get("group_by") or []]
    if intent.get("sort_by"):
        sig["order"] = {"col": intent.get("sort_by"), "desc": bool(intent.get("sort_desc", True))}
    return sig


def signature_text(sig: Dict[str, Any]) -> str:
    return json.dumps(sig or {}, sort_keys=True, separators=(",", ":"))


def signature_sha(sig: Dict[str, Any]) -> str:
    return hashlib.sha1(signature_text(sig).encode("utf-8")).hexdigest()


# --- Minimal eq parser used by rate_grammar ---------------------------------

import re


def _norm_val(v: str) -> str:
    return (v or "").strip()


def parse_eq(text: str) -> Dict[str, Dict[str, List[str]]]:
    """
    Parse occurrences like:
      eq: COL = v1 | v2 | v3
      eq: COL = v1, v2, v3
    Returns a dict mapping column to {op:'in', values:[...]}
    """
    out: Dict[str, Dict[str, List[str]]] = {}
    if not text:
        return out
    pattern = re.compile(r"(?i)\beq\s*:\s*([A-Z0-9_]+)\s*=\s*([^;]+)")
    for m in pattern.finditer(text):
        col = (m.group(1) or "").upper().strip()
        rhs = (m.group(2) or "").strip()
        parts = re.split(r"\s*\|\s*|,\s*", rhs)
        vals = [_norm_val(p) for p in parts if _norm_val(p)]
        if not vals:
            continue
        entry = out.setdefault(col, {"op": "in", "values": []})
        entry["values"].extend(vals)
        # de-dup while preserving order
        seen: set[str] = set()
        entry["values"] = [x for x in entry["values"] if not (x in seen or seen.add(x))]
    return out
