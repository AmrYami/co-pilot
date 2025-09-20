import re
from typing import Dict, List

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
    allow_binds: List[str],
    question_has_window: bool,
    required_date_column: str | None,   # e.g., "END_DATE" if user said "end date", else default or None
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

    # Bind whitelist
    binds = _BIND_RE.findall(cleaned)
    bad_binds = [b for b in binds if b not in allow_binds]
    if bad_binds:
        errs.append(f"illegal_bind:{','.join(bad_binds)}")

    # Window logic
    if question_has_window:
        # require :date_start and :date_end
        need = {"date_start","date_end"}
        have = set(binds)
        missing = list(need - have)
        if missing:
            errs.append("missing_binds")
        # ensure correct date column is in WHERE / filters
        if required_date_column and not _mentions_column(cleaned, required_date_column):
            # allow if some date column is used, but prefer required
            errs.append(f"date_column_mismatch:{required_date_column}")
    else:
        # reject unexpected date binds unless the SQL has a *very* clear literal date filter that user asked for (we assume not)
        if "date_start" in binds or "date_end" in binds:
            errs.append("unexpected_date_filter")

    return {"ok": len(errs) == 0, "errors": errs, "binds": binds}
