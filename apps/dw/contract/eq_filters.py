import re
from typing import Dict, List, Tuple

GROSS_EXPR = (
    "NVL(CONTRACT_VALUE_NET_OF_VAT,0) + "
    "CASE WHEN NVL(VAT,0) BETWEEN 0 AND 1 "
    "THEN NVL(CONTRACT_VALUE_NET_OF_VAT,0) * NVL(VAT,0) "
    "ELSE NVL(VAT,0) END"
)

def _norm_ident(s: str) -> str:
    return re.sub(r"\s+", "_", s.strip()).upper()

def detect_eq_filters(question: str,
                      table: str,
                      explicit_cols: List[str],
                      enum_synonyms: Dict[str, Dict]) -> List[Tuple[str, Dict]]:
    """
    Returns a list of (column, match_spec) from patterns like 'REQUEST TYPE = Renewal'
    match_spec contains either 'equals': [..], 'prefix': [..], 'contains': [..] derived from DW_ENUM_SYNONYMS
    or simple 'value' for plain equality (case-insensitive, trimmed).
    """
    out = []
    # generic: COLUMN = 'value' | "value" | value
    pat = re.compile(r"\b([A-Za-z_ ][A-Za-z0-9_ ]*?)\s*=\s*(?:'([^']*)'|\"([^\"]*)\"|([^\s,;]+))", re.IGNORECASE)
    for m in pat.finditer(question):
        col_raw, v1, v2, v3 = m.groups()
        col = _norm_ident(col_raw)
        val = (v1 or v2 or v3 or "").strip()
        if col not in [c.upper() for c in explicit_cols]:
            continue
        key = f"{table}.{col}"
        syn = enum_synonyms.get(key, enum_synonyms.get(col, {}))
        if syn:
            # try to map normalized value to a synonym bucket (lowercase key)
            norm_val = val.strip().lower()
            bucket = syn.get(norm_val)
            if not bucket:
                for name, spec in syn.items():
                    if isinstance(name, str) and name.strip().lower() == norm_val:
                        bucket = spec
                        break
            if bucket:
                out.append((col, {
                    "equals": bucket.get("equals", []),
                    "prefix": bucket.get("prefix", []),
                    "contains": bucket.get("contains", [])
                }))
                continue
        # fall back to simple equality
        out.append((col, {"value": val}))
    return out

def build_eq_sql(col: str, spec: Dict, bind_maker) -> Tuple[str, Dict]:
    """
    Builds SQL predicate and binds for equality/spec with synonyms. Case-insensitive & trimmed.
    """
    binds = {}
    if "value" in spec:
        b = bind_maker(f"eq_{col.lower()}")
        binds[b] = spec["value"]
        pred = f"TRIM(UPPER({col})) = TRIM(UPPER(:{b}))"
        return pred, binds

    clauses = []
    # equals list
    for i, v in enumerate(spec.get("equals", [])):
        b = bind_maker(f"eq_{col.lower()}_{i}")
        binds[b] = v
        clauses.append(f"TRIM(UPPER({col})) = TRIM(UPPER(:{b}))")
    # prefix list -> LIKE 'prefix%'
    for i, v in enumerate(spec.get("prefix", [])):
        b = bind_maker(f"pre_{col.lower()}_{i}")
        binds[b] = f"{v}%"
        clauses.append(f"UPPER(TRIM({col})) LIKE UPPER(:{b})")
    # contains list -> LIKE '%token%'
    for i, v in enumerate(spec.get("contains", [])):
        b = bind_maker(f"con_{col.lower()}_{i}")
        binds[b] = f"%{v}%"
        clauses.append(f"UPPER(TRIM({col})) LIKE UPPER(:{b})")
    pred = "(" + " OR ".join(clauses) + ")"
    return pred, binds
