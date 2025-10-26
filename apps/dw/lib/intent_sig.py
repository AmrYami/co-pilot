# apps/dw/lib/intent_sig.py
import json
import re
import hashlib
from typing import Any, Dict, List, Tuple

_EMAIL_RX = re.compile(r"(?i)\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b")
_PHONE_RX = re.compile(r"\b(?:\+?\d[\d\s\-]{6,})\b")


def is_email(s: str) -> bool:
    return bool(_EMAIL_RX.search(s or ""))


def is_phone(s: str) -> bool:
    return bool(_PHONE_RX.search(s or ""))


def _norm_val(v: Any) -> str:
    if v is None:
        return ""
    text = str(v).strip()
    if not text:
        return ""
    if is_email(text):
        return text.lower()
    if is_phone(text):
        # keep digits only for phones
        return re.sub(r"\D+", "", text)
    return text.lower()


def _norm_eq_filters(eq_filters: List[List[Any]]) -> Dict[str, List[str]]:
    # eq_filters shape: [[COL, [v1,v2,...]], ...]
    out: Dict[str, List[str]] = {}
    for pair in (eq_filters or []):
        try:
            col, values = pair[0], pair[1]
        except Exception:
            continue
        col_u = (str(col or "").upper().strip())
        if not col_u:
            continue
        vals = sorted({
            _norm_val(x)
            for x in (values or [])
            if str(x or "").strip()
        })
        if vals:
            out[col_u] = vals
    return out


def _norm_fts(fts_groups: List[List[str]]) -> List[str]:
    # fts_groups: [["it"], ["home care"], ...]
    toks: List[str] = []
    for g in (fts_groups or []):
        for t in (g or []):
            if t and not (is_email(t) or is_phone(t)):
                toks.append(_norm_val(t))
    return sorted({t for t in toks if t})


def build_intent_signature(intent: Dict[str, Any]) -> Tuple[Dict[str, Any], str, str]:
    sig_dict: Dict[str, Any] = {
        "eq": _norm_eq_filters(intent.get("eq_filters") or []),
        "fts": _norm_fts(intent.get("fts_groups") or []),
        "group_by": sorted([g for g in (intent.get("group_by") or [])]),
        "order": {
            "col": (intent.get("sort_by") or "REQUEST_DATE"),
            "desc": bool(intent.get("sort_desc", True)),
        },
    }
    sig_str = json.dumps(sig_dict, sort_keys=True, ensure_ascii=False)
    sha = hashlib.sha1(sig_str.encode("utf-8")).hexdigest()
    return sig_dict, sig_str, sha

