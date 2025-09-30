from __future__ import annotations

import re
from typing import Dict, List, Tuple

from .semantics import expand_status

# Column aliases to normalize user-typed column names
_COLUMN_ALIASES = {
    "REQUEST TYPE": "REQUEST_TYPE",
    "REQUEST-TYPE": "REQUEST_TYPE",
    "REQUESTTYPE": "REQUEST_TYPE",
    "CONTRACT STATUS": "CONTRACT_STATUS",
    "CONTRACT-STATUS": "CONTRACT_STATUS",
    "OWNER DEPARTMENT": "OWNER_DEPARTMENT",
    "OWNER-DEPARTMENT": "OWNER_DEPARTMENT",
    "DEPARTMENT OUL": "DEPARTMENT_OUL",
    "DEPARTMENT-OUL": "DEPARTMENT_OUL",
}


def _normalize_col_token(tok: str) -> str:
    """Normalize tokens that look like column names into canonical form."""

    t = re.sub(r"\s+", " ", tok or "").strip()
    t_up = t.upper()
    if t_up in _COLUMN_ALIASES:
        return _COLUMN_ALIASES[t_up]
    # also accept turning spaces to underscore
    t_underscore = t_up.replace(" ", "_")
    if t_underscore in _COLUMN_ALIASES:
        return _COLUMN_ALIASES[t_underscore]
    return t_underscore  # fallback like REQUEST TYPE -> REQUEST_TYPE


def _get_enum_synonyms(settings: Dict[str, object] | None, table: str, column: str) -> Dict[str, Dict[str, List[str]]]:
    """Load enum synonyms from settings and normalize payload shape."""

    res: Dict[str, Dict[str, List[str]]] = {}

    enum_all = (settings or {}).get("DW_ENUM_SYNONYMS") or {}
    key = f"{table}.{column}"
    if key in enum_all:
        spec = enum_all[key] or {}
        for cat, payload in spec.items():
            cat_l = (cat or "").lower().strip()
            eqs = [e for e in (payload or {}).get("equals", []) or []]
            pfx = [p for p in (payload or {}).get("prefix", []) or []]
            ctn = [c for c in (payload or {}).get("contains", []) or []]
            res[cat_l] = {"equals": eqs, "prefix": pfx, "contains": ctn}

    # Back-compat only for REQUEST_TYPE
    if column.upper() == "REQUEST_TYPE":
        rt_syn = (settings or {}).get("DW_REQUEST_TYPE_SYNONYMS") or {}
        for cat, vals in rt_syn.items():
            cat_l = (cat or "").lower().strip()
            eqs = res.get(cat_l, {}).get("equals", [])
            merged = list(dict.fromkeys(eqs + (vals or [])))  # uniq while preserving order
            cur = res.get(cat_l, {"equals": [], "prefix": [], "contains": []})
            cur["equals"] = merged
            res[cat_l] = cur

    return res


def _match_enum_category(value: str, enum_syn: Dict[str, Dict[str, List[str]]]):
    """Return category + payload when value matches category name or equals list."""

    v = (value or "").strip().lower()
    if not v:
        return None, None
    for cat_l, payload in (enum_syn or {}).items():
        if v == cat_l:
            return cat_l, payload
        # equals match
        eqs = [e.strip().lower() for e in payload.get("equals", []) or []]
        if v in eqs:
            return cat_l, payload
    return None, None


def build_request_type_filter(question: str, settings: Dict[str, object] | None):
    """
    Detect explicit REQUEST_TYPE comparisons inside the natural-language question.
    Returns a tuple of (sql_fragment, binds_dict, explain_string) or (None, {}, None).
    """

    q = (question or "")
    # coarse regex to capture "REQUEST TYPE" or "REQUEST_TYPE" comparisons
    # Examples matched: "REQUEST TYPE = Renewal", "REQUEST_TYPE: renewal", "request type is renew"
    m = re.search(r"(REQUEST[\s_-]?TYPE)\s*(=|:|is|equals)?\s*([\"']?)([^\"']+)\3", q, flags=re.I)
    if not m:
        return None, {}, None

    raw_col = m.group(1) or ""
    val = (m.group(4) or "").strip()
    col = _normalize_col_token(raw_col)
    if col != "REQUEST_TYPE":
        # only handle REQUEST_TYPE here; other columns can have their own specialized filters if needed
        return None, {}, None

    enum_syn = _get_enum_synonyms(settings, table="Contract", column="REQUEST_TYPE")
    cat, payload = _match_enum_category(val, enum_syn)

    binds: Dict[str, object] = {}
    like_parts: List[str] = []
    equals_upper: List[str] = []

    if payload:
        # equals list → exact matches (case-insensitive)
        for idx, s in enumerate(payload.get("equals", []) or []):
            if not s:
                continue
            bname = f"rt_eq_{idx}"
            binds[bname] = s.upper()
            equals_upper.append(f":{bname}")

        # prefix list → LIKE 'VALUE%'
        for idx, s in enumerate(payload.get("prefix", []) or []):
            if not s:
                continue
            bname = f"rt_p_{idx}"
            binds[bname] = s.upper() + "%"
            like_parts.append(f"UPPER(REQUEST_TYPE) LIKE :{bname}")

        # contains list → LIKE '%VALUE%'
        for idx, s in enumerate(payload.get("contains", []) or []):
            if not s:
                continue
            bname = f"rt_c_{idx}"
            binds[bname] = "%" + s.upper() + "%"
            like_parts.append(f"UPPER(REQUEST_TYPE) LIKE :{bname}")

    # Fallback when the value itself is given but no category matched: do contains OR prefix with the raw value
    if not equals_upper and not like_parts:
        b_like = "rt_like_0"
        binds[b_like] = f"%{val.upper()}%"
        like_parts.append(f"UPPER(REQUEST_TYPE) LIKE :{b_like}")

    clauses: List[str] = []
    if equals_upper:
        # Note: bind values are already uppercased; compare against UPPER(REQUEST_TYPE)
        # Oracle cannot do UPPER() inside IN(...) for each bind easily, so we keep binds upper and compare UPPER(col)
        in_list = ", ".join(equals_upper)
        clauses.append(f"UPPER(REQUEST_TYPE) IN ({in_list})")
    if like_parts:
        clauses.append("(" + " OR ".join(like_parts) + ")")

    if not clauses:
        return None, {}, None

    filter_sql = "(" + " OR ".join(clauses) + ")"
    explain = f"Filter on REQUEST_TYPE using synonyms for '{val}'."
    return filter_sql, binds, explain


# Regex for explicit "column = value" patterns in natural language
_EQ_PATTERNS = [
    re.compile(r"\bwhere\s+([A-Z_][A-Z0-9_]*)\s*=\s*([^\s,;]+)", re.I),
    re.compile(r"\b([A-Z_][A-Z0-9_]*)\s*=\s*([^\s,;]+)", re.I),
    re.compile(r"\b([A-Z_][A-Z0-9_]*)\s+(?:equals|is)\s*([^\s,;]+)", re.I),
]


def parse_explicit_filters(
    question: str,
    allowed_columns: List[str],
) -> Tuple[List[str], Dict[str, object]]:
    """Extract explicit column=value constraints. Return SQL snippets and bind dict."""

    q = (question or "").strip()
    if not q:
        return [], {}

    colset = {c.upper().strip(): c for c in (allowed_columns or [])}
    snippets: List[str] = []
    binds: Dict[str, object] = {}
    bind_idx = 0

    def next_bind_name() -> str:
        nonlocal bind_idx
        name = f"p{bind_idx}"
        bind_idx += 1
        return name

    for pattern in _EQ_PATTERNS:
        for match in pattern.finditer(q):
            col_raw, val_raw = match.group(1), match.group(2)
            col_key = col_raw.upper()
            if col_key not in colset:
                continue
            col = colset[col_key]
            value = val_raw.strip().strip('"').strip("'")

            if col_key == "CONTRACT_STATUS":
                alts = expand_status(value)
                bind_names = []
                for alt in alts:
                    bind_name = next_bind_name()
                    bind_names.append(f":{bind_name}")
                    binds[bind_name] = alt
                snippets.append(
                    "UPPER(TRIM(CONTRACT_STATUS)) IN (" + ", ".join(bind_names) + ")"
                )
            else:
                bind_name = next_bind_name()
                binds[bind_name] = value
                snippets.append(
                    f"UPPER(TRIM({col})) = UPPER(TRIM(:{bind_name}))"
                )

    return snippets, binds
