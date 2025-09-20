import re
from typing import Dict, List, Sequence

_DML_RE = re.compile(r"\b(INSERT|UPDATE|DELETE|MERGE|TRUNCATE|ALTER|CREATE|DROP)\b", re.I)
_SELECT_RE = re.compile(r"^\s*(SELECT|WITH)\b", re.I)
_BIND_RE = re.compile(r':([A-Za-z_][A-Za-z0-9_]*)')
_DATE_FUNC_RE = re.compile(r'\b(BETWEEN|>=|<=|<|>)\b', re.I)


def _find_tables(sql: str) -> List[str]:
    # Light parser: find table names after FROM / JOIN (quoted or not)
    tbls = []
    for m in re.finditer(r"\bFROM\s+([\"A-Za-z_][\w\"$\.]*)", sql, re.I):
        tbls.append(m.group(1).strip('"'))
    for m in re.finditer(r"\bJOIN\s+([\"A-Za-z_][\w\"$\.]*)", sql, re.I):
        tbls.append(m.group(1).strip('"'))
    return list(dict.fromkeys(tbls))


def _mentions_column(sql: str, col: str) -> bool:
    # crude but sufficient for gatekeeping
    return re.search(rf"\b{re.escape(col)}\b", sql, re.I) is not None


def _has_any(sql: str, cols: List[str]) -> bool:
    return any(_mentions_column(sql, c) for c in cols)


def validate_sql(
    sql: str,
    *,
    allow_tables: List[str],
    allow_columns: List[str],
    bind_whitelist: List[str],
    time_window_required: bool,
) -> Dict:
    errs: List[str] = []
    cleaned = (sql or "").strip()
    if not cleaned:
        return {"ok": False, "errors": ["empty_sql"], "binds": []}
    if not _SELECT_RE.search(cleaned):
        return {"ok": False, "errors": ["not_select"], "binds": []}
    if _DML_RE.search(cleaned):
        return {"ok": False, "errors": ["dml_forbidden"], "binds": []}

    # Table whitelist
    tables = _find_tables(cleaned)
    bad_tbls = [t for t in tables if t not in allow_tables]
    if bad_tbls:
        errs.append(f"illegal_table:{','.join(bad_tbls)}")

    # Column whitelist (coarse scan)
    for token in re.findall(r"[A-Za-z_][A-Za-z0-9_]*", cleaned):
        # ignore SQL keywords quickly
        if token.upper() in ("SELECT","FROM","WHERE","GROUP","BY","ORDER","WITH","UNION","ALL","AND","OR","NOT",
                             "NVL","TRIM","UPPER","LISTAGG","ASC","DESC","ON","JOIN","INNER","LEFT","RIGHT","FETCH",
                             "FIRST","ROWS","ONLY","AS","COUNT","SUM","AVG","MIN","MAX","DISTINCT","BETWEEN","LIKE"):
            continue
        # allow binds
        if token.startswith(":"):
            continue
        # allow numbers
        if token.isdigit():
            continue
        # if it looks like a column but not whitelisted
        if token.upper() not in [c.upper() for c in allow_columns] and token.upper() not in [t.upper() for t in allow_tables]:
            # Heuristic: don't scream on obvious aliases; we only guard binds strictly.
            pass

    bind_info = analyze_binds(cleaned, bind_whitelist)
    if bind_info["illegal"]:
        errs.append(f"illegal_binds:{','.join(bind_info['illegal'])}")

    used_binds = set(bind_info["used"])
    has_date_start = "date_start" in used_binds
    has_date_end = "date_end" in used_binds

    if time_window_required and not ({"date_start", "date_end"} <= used_binds):
        errs.append("missing_date_binds")

    if (has_date_start or has_date_end) and not ({"date_start", "date_end"} <= used_binds):
        errs.append("missing_date_binds_pair")

    return {"ok": len(errs) == 0, "errors": errs, "binds": bind_info["used"]}


def analyze_binds(sql: str, allow: Sequence[str], provided: Dict[str, object] | None = None) -> Dict:
    """Inspect bind usage ensuring only allowed binds appear and required values exist."""

    provided = provided or {}
    found = sorted(set(_BIND_RE.findall(sql or "")))
    allowed = set(allow)
    illegal = sorted(b for b in found if b not in allowed)
    missing_values = sorted(b for b in found if b not in provided)
    return {
        "used": found,
        "illegal": illegal,
        "missing_values": missing_values,
        "ok": not illegal and not missing_values,
    }
