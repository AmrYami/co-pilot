import json
import re
from typing import Dict, List, Tuple, Any

EMAIL_RE = re.compile(r"(?i)\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b")
PHONE_RE = re.compile(r"\b\+?\d{7,15}\b")


def is_email(s: str) -> bool:
    return bool(EMAIL_RE.search(s or ""))


def is_phone(s: str) -> bool:
    return bool(PHONE_RE.search(s or ""))


def _norm_val(v: str) -> str:
    # Keep original case; SQL side applies UPPER(:bind) when needed
    return (v or "").strip()


def parse_eq(text: str) -> Dict[str, Dict[str, List[str]]]:
    """
    Supports multi-value equality hints inside /dw/rate comments.
      eq: COL = v1 | v2 | v3
      eq: COL = v1, v2, v3
    Returns: { "COL": {"op": "in", "values": [..]} }
    """
    out: Dict[str, Dict[str, List[str]]] = {}
    for m in re.finditer(r"(?i)\beq\s*:\s*([A-Z0-9_]+)\s*=\s*([^;]+)", text or ""):
        col = (m.group(1) or "").upper().strip()
        rhs = (m.group(2) or "").strip()
        parts = re.split(r"\s*\|\s*|,\s*", rhs)
        vals = [_norm_val(p) for p in parts if p and _norm_val(p)]
        if not vals:
            continue
        entry = out.setdefault(col, {"op": "in", "values": []})
        entry["values"].extend(vals)
        # deduplicate preserving order
        seen: set[str] = set()
        entry["values"] = [x for x in entry["values"] if not (x in seen or seen.add(x))]
    return out


def parse_or_group(text: str) -> List[Dict[str, Dict[str, List[str]]]]:
    """
    or_group: (eq: COL1 = A | B), (eq: COL2 = C | D)
    Returns a list of group dicts. Each group is OR'd with the next.
    """
    groups: List[Dict[str, Dict[str, List[str]]]] = []
    for g in re.finditer(r"(?is)or_group\s*:\s*\((.*?)\)(?:\s*,\s*\((.*?)\))*", text or ""):
        whole = g.group(0) or ""
        inner = re.findall(r"\((.*?)\)", whole)
        grp: Dict[str, Dict[str, List[str]]] = {}
        for blob in inner:
            sub = parse_eq(blob)
            for c, spec in sub.items():
                dst = grp.setdefault(c, {"op": "in", "values": []})
                dst["values"].extend(spec.get("values") or [])
        for c, spec in list(grp.items()):
            seen: set[str] = set()
            vals = spec.get("values") or []
            grp[c]["values"] = [x for x in vals if not (x in seen or seen.add(x))]
        if grp:
            groups.append(grp)
    return groups


def build_intent_signature(intent: Dict[str, Any]) -> str:
    """
    Deterministic signature JSON for an intent. Sorts lists/keys to ensure stability.
    Expected subset shape:
      intent = {
        "eq_filters": [["ENTITY", ["DSFH"]], ["REPRESENTATIVE_EMAIL", ["a@x","b@y"]]],
        "fts_groups": [["it"],["home care"]],
        "sort_by": "REQUEST_DATE", "sort_desc": True,
        "group_by": ["ENTITY"],
      }
    """
    sig = {"eq": {}, "fts": [], "order": {}, "group_by": []}

    # eq
    for pair in intent.get("eq_filters", []) or []:
        try:
            col, vals = pair[0], pair[1]
        except Exception:
            continue
        col_u = (col or "").upper().strip()
        vals_n = [_norm_val(v) for v in (vals or []) if _norm_val(v)]
        sig["eq"][col_u] = sorted(set(vals_n))

    # fts
    for grp in intent.get("fts_groups", []) or []:
        toks = [_norm_val(t) for t in (grp or []) if _norm_val(t)]
        if toks:
            sig["fts"].append(sorted(set(toks)))

    # order
    sig["order"] = {
        "col": (intent.get("sort_by") or "").upper(),
        "desc": bool(intent.get("sort_desc", True)),
    }

    # group_by
    gcols = intent.get("group_by") or []
    if isinstance(gcols, str):
        parts = [gcols]
    else:
        parts = list(gcols)
    sig["group_by"] = sorted({(c or "").upper() for c in parts if (c or "").strip()})

    return json.dumps(sig, sort_keys=True, ensure_ascii=False)

