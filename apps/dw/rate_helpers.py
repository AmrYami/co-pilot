import re
from typing import Dict, List, Tuple


# Safe fallback FTS builder on standard columns using LIKEs
def build_fts_like_where(columns: List[str], tokens: List[str], operator: str = "OR") -> Tuple[str, Dict[str, str]]:
    """
    Build a SQL WHERE clause for FTS using LIKE on the provided columns.
    operator: 'OR' (default) or 'AND' between token groups.
    Returns (sql_fragment, binds).
    """
    binds: Dict[str, str] = {}
    groups = []
    for i, tok in enumerate(tokens or []):
        normalized = (tok or "").strip()
        if not normalized:
            continue
        bind = f"fts_{i}"
        binds[bind] = f"%{normalized}%"
        per_col = [f"UPPER(NVL({col},'')) LIKE UPPER(:{bind})" for col in columns]
        groups.append("(" + " OR ".join(per_col) + ")")
    if not groups:
        return "", {}
    glue = " OR " if (operator or "OR").upper() == "OR" else " AND "
    return "(" + glue.join(groups) + ")", binds


EQ_RE = re.compile(
    r"""eq\s*:\s*
        (?P<col>[A-Za-z0-9_]+)        # column
        \s*=\s*
        (?P<val>[^;]+?)               # value until ';' (non-greedy)
        (?:\s*\(\s*(?P<flags>[^\)]*)\))?   # optional flags like (ci, trim)
        \s*(?:;|$)""",
    re.IGNORECASE | re.VERBOSE,
)

FTS_RE = re.compile(r"""fts\s*:\s*(?P<body>[^;]+)""", re.IGNORECASE)
ORDER_RE = re.compile(r"""order_by\s*:\s*(?P<col>[A-Za-z0-9_]+)(?:\s+(?P<dir>asc|desc))?""", re.IGNORECASE)
GB_RE = re.compile(r"""group_by\s*:\s*(?P<cols>[A-Za-z0-9_,\s]+)""", re.IGNORECASE)
GROSS_RE = re.compile(r"""gross\s*:\s*(?P<flag>true|false)""", re.IGNORECASE)


def _parse_flags(flags_str: str) -> Dict[str, bool]:
    flags = {"ci": False, "trim": False}
    for part in [p.strip().lower() for p in (flags_str or "").split(",") if p.strip()]:
        if part == "ci":
            flags["ci"] = True
        elif part == "trim":
            flags["trim"] = True
    return flags


def parse_rate_comment(comment: str) -> Dict:
    """
    Parse /dw/rate 'comment' grammar into a normalized dict.
    Supports:
      - fts: token1 | token2
      - eq: COL = VAL (ci, trim)
      - group_by: COL1, COL2
      - order_by: COL [asc|desc]
      - gross: true|false
    """
    result = {
        "fts_tokens": [],
        "fts_operator": "OR",
        "fts_reason": None,
        "eq_filters": [],  # list of {col, val, ci, trim, op}
        "order_by": None,  # {col, dir}
        "group_by": [],
        "gross": None,
    }
    if not comment:
        return result

    text = comment or ""

    # fts:
    m = FTS_RE.search(text)
    if m:
        body = (m.group("body") or "").strip()
        # Decide operator: explicit 'and' wins when no explicit '|'
        if "|" in body:
            tokens_raw = [frag.strip() for frag in body.split("|")]
            result["fts_operator"] = "OR"
            result["fts_reason"] = "OR because '|' separator detected"
        elif re.search(r"\band\b", body, flags=re.IGNORECASE):
            tokens_raw = [frag.strip() for frag in re.split(r"\band\b", body, flags=re.IGNORECASE)]
            result["fts_operator"] = "AND"
            result["fts_reason"] = "AND because keyword 'and' was present"
        else:
            tokens_raw = [frag.strip() for frag in body.split("|")]
            result["fts_operator"] = "OR"
            result["fts_reason"] = "OR default"
        cleaned = []
        for raw_tok in tokens_raw:
            token = raw_tok.strip(" '\"")
            if token:
                cleaned.append(token)
        result["fts_tokens"] = cleaned

    # eq:
    for em in EQ_RE.finditer(text):
        col = (em.group("col") or "").strip().upper()
        raw_val = (em.group("val") or "").strip()
        val = raw_val.strip().strip("'").strip('"')
        flags = {"ci": True, "trim": True}
        if em.group("flags"):
            parsed_flags = _parse_flags(em.group("flags"))
            flags.update(parsed_flags)
        if col and val:
            result["eq_filters"].append({"col": col, "val": val, "ci": flags.get("ci", True), "trim": flags.get("trim", True), "op": "eq"})

    # group_by:
    m = GB_RE.search(text)
    if m:
        cols = [c.strip().upper() for c in (m.group("cols") or "").split(",") if c.strip()]
        result["group_by"] = cols

    # order_by:
    m = ORDER_RE.search(text)
    if m:
        col = (m.group("col") or "").strip().upper()
        direction = ((m.group("dir") or "DESC").upper())
        result["order_by"] = {"col": col, "dir": direction}

    # gross:
    m = GROSS_RE.search(text)
    if m:
        result["gross"] = m.group("flag").lower() == "true"

    return result


def build_eq_clause(eq_filters: List[Dict]) -> Tuple[str, Dict[str, str]]:
    """
    Build ANDed equality predicates for provided eq_filters.
    Returns (sql_fragment, binds)
    """
    if not eq_filters:
        return "", {}
    conds = []
    binds: Dict[str, str] = {}
    for i, f in enumerate(eq_filters):
        col = f.get("col")
        val = f.get("val")
        if not col or val is None:
            continue
        b = f"eq_{i}"
        binds[b] = val
        ci = f.get("ci", True)
        trim = f.get("trim", True)
        left = col
        right = f":{b}"
        if trim and ci:
            conds.append(f"UPPER(TRIM({left})) = UPPER(TRIM({right}))")
        elif trim and not ci:
            conds.append(f"TRIM({left}) = TRIM({right})")
        elif ci and not trim:
            conds.append(f"UPPER({left}) = UPPER({right})")
        else:
            conds.append(f"{left} = {right}")
    if not conds:
        return "", {}
    return "(" + " AND ".join(conds) + ")", binds


def choose_fts_columns(settings: Dict) -> List[str]:
    """
    Choose FTS columns from settings.DW_FTS_COLUMNS for 'Contract' or fallback '*'.
    Always return a non-empty list with safe defaults.
    """
    cols: List[str] = []
    try:
        mapping = settings.get("DW_FTS_COLUMNS", {}) if settings else {}
        if isinstance(mapping, dict):
            contract_cols = mapping.get("Contract") or mapping.get("CONTRACT")
            if isinstance(contract_cols, list) and contract_cols:
                cols = contract_cols
            elif "*" in mapping and isinstance(mapping.get("*"), list) and mapping.get("*"):
                cols = mapping.get("*")  # type: ignore[assignment]
    except Exception:
        cols = []
    if not cols:
        cols = ["CONTRACT_SUBJECT", "CONTRACT_PURPOSE"]
    return [c.upper() for c in cols if isinstance(c, str) and c.strip()]
