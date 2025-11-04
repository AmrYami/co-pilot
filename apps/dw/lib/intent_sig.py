# apps/dw/lib/intent_sig.py
import json
import re
import hashlib
from typing import Any, Dict, List, Tuple

try:
    from apps.dw.sql_shared import eq_alias_columns
except Exception:  # pragma: no cover - settings not wired during isolated import
    eq_alias_columns = None  # type: ignore

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
    alias_map = {}
    if callable(eq_alias_columns):
        try:
            alias_map = eq_alias_columns() or {}
        except Exception:
            alias_map = {}

    alias_targets_index: Dict[str, Tuple[str, ...]] = {}
    canonical_for_targets: Dict[Tuple[str, ...], str] = {}

    def _score_alias(name: str) -> Tuple[int, int, str]:
        text = name or ""
        return (
            1 if text.endswith("S") and not text.endswith("SS") else 0,
            len(text),
            text,
        )

    for alias, cols in (alias_map or {}).items():
        alias_key = str(alias or "").strip().upper()
        cols_tuple = tuple(sorted(str(c or "").strip().upper() for c in (cols or []) if str(c or "").strip()))
        alias_targets_index[alias_key] = cols_tuple
        if not cols_tuple:
            continue
        current = canonical_for_targets.get(cols_tuple)
        if current is None or _score_alias(alias_key) > _score_alias(current):
            canonical_for_targets[cols_tuple] = alias_key

    def _canonical_alias(name: str) -> str:
        alias_key = str(name or "").strip().upper()
        cols_tuple = alias_targets_index.get(alias_key)
        if not cols_tuple:
            return alias_key
        return canonical_for_targets.get(cols_tuple, alias_key)

    for pair in (eq_filters or []):
        try:
            col, values = pair[0], pair[1]
        except Exception:
            continue
        col_u = _canonical_alias(str(col or "").upper().strip())
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


def _norm_aggs(items: Any) -> List[Dict[str, Any]]:
    normalized: List[Dict[str, Any]] = []
    seen: set[Tuple[str, str, bool]] = set()
    for entry in items or []:
        if not isinstance(entry, dict):
            continue
        func = str(entry.get("func") or "").strip().upper()
        if not func:
            continue
        column_raw = entry.get("column")
        if column_raw == "*" or str(column_raw or "").strip() == "*":
            column = "*"
        else:
            column = str(column_raw or "").strip().upper()
        distinct = bool(entry.get("distinct"))
        key = (func, column, distinct)
        if key in seen:
            continue
        seen.add(key)
        normalized.append(
            {
                "func": func,
                "column": column if column else "*",
                "distinct": distinct,
            }
        )
    normalized.sort(key=lambda item: (item["func"], item["column"], item["distinct"]))
    return normalized


def build_intent_signature(intent: Dict[str, Any]) -> Tuple[Dict[str, Any], str, str]:
    sig_dict: Dict[str, Any] = {
        "eq": _norm_eq_filters(intent.get("eq_filters") or []),
        "fts": _norm_fts(intent.get("fts_groups") or []),
        "group_by": sorted(
            {
                str(g or "").strip().upper()
                for g in (intent.get("group_by") or [])
                if isinstance(g, str) and str(g or "").strip()
            }
        ),
        "order": {
            "col": (intent.get("sort_by") or "REQUEST_DATE"),
            "desc": bool(intent.get("sort_desc", True)),
        },
        "agg": _norm_aggs(intent.get("aggregations")),
    }
    sig_str = json.dumps(sig_dict, sort_keys=True, ensure_ascii=False)
    sha = hashlib.sha1(sig_str.encode("utf-8")).hexdigest()
    return sig_dict, sig_str, sha
