# -*- coding: utf-8 -*-
"""
Generic equality detector with synonyms support for Contract.REQUEST_TYPE.
Respects:
  - DW_EXPLICIT_FILTER_COLUMNS
  - DW_ENUM_SYNONYMS["Contract.REQUEST_TYPE"]
"""
import re
from typing import Dict, List, Tuple

_EQ_PATTERNS = [
    r"(?P<col>[A-Za-z0-9_ ]+?)\s*=\s*(?P<val>.+)",
    r"(?P<col>[A-Za-z0-9_ ]+?)\s*==\s*(?P<val>.+)",
    r"(?P<col>[A-Za-z0-9_ ]+?)\s+is\s+(?P<val>.+)",
    r"(?P<col>[A-Za-z0-9_ ]+?)\s+equals\s+(?P<val>.+)",
]


def _clean_val(v: str) -> str:
    v = (v or "").strip()
    # strip quotes if present
    if len(v) >= 2 and ((v[0] == v[-1]) and v[0] in ("'", '"')):
        v = v[1:-1].strip()
    return v


def _normalize_col(col: str) -> str:
    return (col or "").strip().upper().replace(" ", "_")


def resolve_explicit_columns(settings: Dict) -> List[str]:
    cfg = (settings or {}).get("DW_EXPLICIT_FILTER_COLUMNS", {}) or {}
    val = cfg.get("value") if isinstance(cfg, dict) else cfg
    cols = [c.strip().upper() for c in (val or []) if isinstance(c, str)]
    return cols


def parse_eq_from_text(text: str, settings: Dict) -> List[Dict]:
    """
    Extract equality-like expressions from free text.
    Returns a list of descriptor dicts: {col, val, ci, trim}
    ci/trim default True for robustness; /dw/rate can override.
    """
    explicit_cols = resolve_explicit_columns(settings)
    results: List[Dict] = []
    t = (text or "")
    for pat in _EQ_PATTERNS:
        for m in re.finditer(pat, t, flags=re.IGNORECASE):
            col = _normalize_col(m.group("col"))
            val = _clean_val(m.group("val"))
            if col in explicit_cols and val:
                results.append({"col": col, "val": val, "ci": True, "trim": True})
    return results


def _gross_expr() -> str:
    # same expression used in your system
    return "NVL(CONTRACT_VALUE_NET_OF_VAT,0) + CASE WHEN NVL(VAT,0) BETWEEN 0 AND 1 THEN NVL(CONTRACT_VALUE_NET_OF_VAT,0) * NVL(VAT,0) ELSE NVL(VAT,0) END"


def _apply_synonyms_request_type(val: str, settings: Dict) -> Tuple[str, List[str], List[str]]:
    """
    If column is REQUEST_TYPE, try to expand synonyms using DW_ENUM_SYNONYMS.
    Returns: mode, equals_list, like_prefixes
    """
    syn_cfg = ((settings or {}).get("DW_ENUM_SYNONYMS") or {}).get("value") or {}
    rt_map = syn_cfg.get("Contract.REQUEST_TYPE", {})
    # unify match key
    lv = (val or "").strip().lower()
    equals_list: List[str] = []
    prefixes: List[str] = []
    for key, rule in rt_map.items():
        # if input looks like category name or matches an equals of that category
        eqs = [s for s in rule.get("equals", []) if isinstance(s, str)]
        prefs = [s for s in rule.get("prefix", []) if isinstance(s, str)]
        contains = [s for s in rule.get("contains", []) if isinstance(s, str)]
        if lv == key.lower() or any(lv == s.lower() for s in eqs):
            equals_list.extend([e.upper() for e in eqs])
            prefixes.extend([p.upper() for p in prefs])
            # contains could also be handled if needed; keeping empty by default
            break
    mode = "plain"
    if equals_list or prefixes:
        mode = "synonym"
    return mode, equals_list, prefixes


def build_eq_where(eq_filters: List[Dict], settings: Dict, bind_prefix="eq") -> Tuple[str, Dict[str, str]]:
    """
    Build AND-combined WHERE expressions for equality filters.
    - Applies REQUEST_TYPE synonyms if present.
    - Honors ci/trim flags per filter descriptor.
    """
    clauses: List[str] = []
    binds: Dict[str, str] = {}
    idx = 0
    for f in eq_filters:
        col = _normalize_col(f.get("col"))
        val = _clean_val(f.get("val"))
        ci = bool(f.get("ci", True))
        trim = bool(f.get("trim", True))
        if not col or not val:
            continue
        left = col
        # REQUEST_TYPE synonyms special handling
        if col == "REQUEST_TYPE":
            mode, eqs, prefs = _apply_synonyms_request_type(val, settings)
            if mode == "synonym":
                parts = []
                if eqs:
                    in_binds = []
                    for v in eqs:
                        pname = f"{bind_prefix}_{idx}"
                        idx += 1
                        binds[pname] = v
                        in_binds.append(f":{pname}")
                    expr_col = f"TRIM({left})" if trim else left
                    if ci:
                        expr_col = f"UPPER({expr_col})"
                    in_list = ", ".join(in_binds)
                    parts.append(f"{expr_col} IN ({in_list})")
                for p in prefs:
                    pname = f"{bind_prefix}_{idx}"
                    idx += 1
                    binds[pname] = f"{p}%"
                    expr_col = f"TRIM({left})" if trim else left
                    if ci:
                        parts.append(f"UPPER({expr_col}) LIKE UPPER(:{pname})")
                    else:
                        parts.append(f"{expr_col} LIKE :{pname}")
                if parts:
                    clauses.append("(" + " OR ".join(parts) + ")")
                continue  # handled
        # Generic equality
        pname = f"{bind_prefix}_{idx}"
        idx += 1
        binds[pname] = val
        col_expr = f"TRIM({left})" if trim else left
        rhs = f"TRIM(:{pname})" if trim else f":{pname}"
        if ci:
            clauses.append(f"UPPER({col_expr}) = UPPER({rhs})")
        else:
            clauses.append(f"{col_expr} = {rhs}")
    if not clauses:
        return "", {}
    return "(" + " AND ".join(clauses) + ")", binds
