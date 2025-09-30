import re
from typing import Dict, List, Tuple, Optional


# ---------- Public API ----------

def build_named_filter_sql(
    question: str,
    table_name: str,
    settings: Dict
) -> Tuple[Optional[str], Dict[str, object], List[str]]:
    """
    Parse 'COLUMN = value' (or 'COLUMN is value' / 'COLUMN: value' / 'COLUMN == value')
    for whitelisted columns and build a safe SQL fragment with binds.
    Returns: (sql_fragment_or_None, binds, explain_notes)
    """
    allowed_cols = _allowed_columns(table_name, settings)
    if not allowed_cols:
        return None, {}, []

    # Extract pairs from text
    pairs = _extract_name_equals_value_pairs(question, allowed_cols)

    if not pairs:
        return None, {}, []

    syn_map = _enum_synonyms_map(settings)

    where_parts: List[str] = []
    binds: Dict[str, object] = {}
    notes: List[str] = []

    for col, raw_value in pairs:
        # Expand via DW_ENUM_SYNONYMS if available
        sql_or, b, note = _expand_with_synonyms(table_name, col, raw_value, syn_map)
        if sql_or is None:
            # Fallback: case-insensitive LIKE on the raw value
            pname = _bind_name(f"nf_{_norm(col)}_raw")
            where_parts.append(f"UPPER(TRIM({col})) LIKE UPPER(:{pname})")
            binds[pname] = f"%{raw_value}%"
            notes.append(f"Applied generic LIKE on {col} for '{raw_value}'.")
        else:
            where_parts.append(f"({sql_or})")
            binds.update(b)
            if note:
                notes.append(note)

    if not where_parts:
        return None, {}, []
    return " AND ".join(where_parts), binds, notes


# ---------- Internals ----------

_EQ_PATTERNS = [
    r"\b([A-Za-z][A-Za-z0-9_ ]{1,60})\s*(?:=|==|:|is|equals)\s*(['\"]?)([^'\"\n\r]+?)\2\b",
]


def _norm(s: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", s.upper())


def _allowed_columns(table_name: str, settings: Dict) -> Dict[str, str]:
    """
    Returns a dict mapping normalized names -> actual column name.
    Priority:
      1) DW_EQ_FILTER_COLUMNS[table] if present
      2) DW_FTS_COLUMNS[table]       as a fallback
    """
    by_table: Dict[str, str] = {}

    eq_cols = (settings or {}).get("DW_EQ_FILTER_COLUMNS", {}).get(table_name)
    if isinstance(eq_cols, list) and eq_cols:
        for c in eq_cols:
            by_table[_norm(c)] = c

    # Fallback to FTS columns if EQ list is not provided
    if not by_table:
        fts_cols = (settings or {}).get("DW_FTS_COLUMNS", {}).get(table_name)
        if isinstance(fts_cols, list):
            for c in fts_cols:
                by_table[_norm(c)] = c

    return by_table


def _extract_name_equals_value_pairs(
    question: str,
    allowed: Dict[str, str]
) -> List[Tuple[str, str]]:
    """
    From text, pick tuples (ActualColumnName, Value) for whitelisted columns.
    Column name match is normalized (spaces vs underscores ignored).
    """
    text = question or ""
    results: List[Tuple[str, str]] = []

    for pat in _EQ_PATTERNS:
        for m in re.finditer(pat, text, flags=re.IGNORECASE):
            raw_col = (m.group(1) or "").strip()
            val = (m.group(3) or "").strip().strip(",.;")
            if not raw_col or not val:
                continue
            key = _norm(raw_col)
            if key in allowed:
                results.append((allowed[key], val))

    return results


def _enum_synonyms_map(settings: Dict) -> Dict[str, dict]:
    """
    Returns the DW_ENUM_SYNONYMS map (case-insensitive keys).
    """
    m = (settings or {}).get("DW_ENUM_SYNONYMS", {}) or {}
    # normalize top-level keys to lower
    normd: Dict[str, dict] = {}
    for k, v in m.items():
        normd[k.lower()] = v
    return normd


def _column_synonyms(table: str, col: str, syn_map: Dict[str, dict]) -> Optional[dict]:
    # Key could be 'Contract.REQUEST_TYPE'
    k = f"{table}.{col}".lower()
    return syn_map.get(k)


def _find_category_for_value(spec: dict, raw_value: str) -> Optional[str]:
    """
    Given the per-column spec (e.g. {'renewal': {'equals': [...], 'prefix': [...], 'contains': [...]}, ...}),
    try to find which category matches the raw value (case-insensitive).
    """
    rv = raw_value.strip()
    rv_up = rv.upper()

    for cat, d in spec.items():
        equals = [str(x) for x in d.get("equals", [])]
        if any(rv_up == str(e).upper() for e in equals):
            return cat

    for cat, d in spec.items():
        prefixes = [str(x) for x in d.get("prefix", [])]
        if any(rv_up.startswith(str(p).upper()) for p in prefixes):
            return cat

    for cat, d in spec.items():
        contains = [str(x) for x in d.get("contains", [])]
        if any(str(c).upper() in rv_up for c in contains):
            return cat

    return None


def _bind_name(base: str, i: Optional[int] = None) -> str:
    return f"{base}_{i}" if i is not None else base


def _expand_with_synonyms(
    table: str,
    col: str,
    raw_value: str,
    syn_map: Dict[str, dict]
) -> Tuple[Optional[str], Dict[str, object], str]:
    """
    Build an OR expression using DW_ENUM_SYNONYMS if available.
    Returns (sql_or_clause_or_None, binds, note).
    """
    spec = _column_synonyms(table, col, syn_map)
    if not spec:
        return None, {}, ""

    cat = _find_category_for_value(spec, raw_value) or raw_value.lower()
    d = spec.get(cat)
    if not d:
        # Sanity: fallback LIKE on raw value
        return None, {}, ""

    parts: List[str] = []
    bind_dict: Dict[str, object] = {}

    # equals -> IN (..)
    eq_vals = [str(x) for x in d.get("equals", [])]
    pf_vals = [str(x) for x in d.get("prefix", [])]
    ct_vals = [str(x) for x in d.get("contains", [])]

    # equals
    eq_bind_names: List[str] = []
    for i, val in enumerate(eq_vals):
        pname = _bind_name(f"nf_{_norm(col)}_eq", i)
        eq_bind_names.append(pname)
        bind_dict[pname] = val

    if eq_bind_names:
        parts.append("UPPER(TRIM({c})) IN ({vals})".format(
            c=col,
            vals=", ".join(f"UPPER(:{p})" for p in eq_bind_names)
        ))

    # prefix
    for i, val in enumerate(pf_vals):
        pname = _bind_name(f"nf_{_norm(col)}_pf", i)
        bind_dict[pname] = f"{val}%"
        parts.append(f"UPPER(TRIM({col})) LIKE UPPER(:{pname})")

    # contains
    for i, val in enumerate(ct_vals):
        pname = _bind_name(f"nf_{_norm(col)}_cf", i)
        bind_dict[pname] = f"%{val}%"
        parts.append(f"UPPER(TRIM({col})) LIKE UPPER(:{pname})")

    # Always add the raw value as a contains if it wasn't already covered
    rv_up = raw_value.upper()
    if rv_up not in (v.upper() for v in eq_vals):
        pname = _bind_name(f"nf_{_norm(col)}_raw")
        bind_dict[pname] = f"%{raw_value}%"
        parts.append(f"UPPER(TRIM({col})) LIKE UPPER(:{pname})")

    if not parts:
        return None, {}, ""

    return " OR ".join(parts), bind_dict, f"Expanded {col}='{raw_value}' via DW_ENUM_SYNONYMS."
