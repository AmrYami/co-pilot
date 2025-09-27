from __future__ import annotations
from typing import Any, Dict, List, Tuple
import datetime as _dt

# ---- SQL fragments (Oracle) ----
GROSS_SQL = (
    "NVL(CONTRACT_VALUE_NET_OF_VAT,0) + "
    "CASE WHEN NVL(VAT,0) BETWEEN 0 AND 1 "
    "THEN NVL(CONTRACT_VALUE_NET_OF_VAT,0) * NVL(VAT,0) ELSE NVL(VAT,0) END"
)

# Overlap predicate: active contracts within [date_start, date_end]
OVERLAP_PRED = (
    "(START_DATE IS NOT NULL AND END_DATE IS NOT NULL "
    "AND START_DATE <= :date_end AND END_DATE >= :date_start)"
)

def coerce_oracle_binds(binds: Dict[str, Any]) -> Dict[str, Any]:
    """
    Ensure Oracle gets proper Python types:
      - date_start / date_end as datetime.date
      - top_n as int
    Strings like '2025-08-31' are converted to date(2025,8,31).
    """
    out = dict(binds or {})
    for k in ("date_start", "date_end"):
        v = out.get(k)
        if isinstance(v, str):
            # Expect ISO yyyy-mm-dd
            y, m, d = v.split("-")
            out[k] = _dt.date(int(y), int(m), int(d))
        elif isinstance(v, _dt.datetime):
            out[k] = v.date()
    if "top_n" in out and isinstance(out["top_n"], str):
        out["top_n"] = int(out["top_n"])
    return out

def explain_window(date_col: str, ds: Any, de: Any) -> str:
    """
    Short English explanation of the chosen window strategy.
    """
    if date_col.upper() == "REQUEST_DATE":
        return f"Interpreting time window on REQUEST_DATE: {ds} .. {de}."
    if date_col.upper() == "OVERLAP":
        return f"Interpreting time window as active overlap: START_DATE..END_DATE within {ds} .. {de}."
    return f"Interpreting time window on {date_col}: {ds} .. {de}."

def build_fts_clause(columns: List[str], tokens: List[str]) -> Tuple[str, Dict[str, Any]]:
    """
    Build a simple Oracle FTS predicate across columns using UPPER LIKE.
    """
    if not columns or not tokens:
        return "", {}
    parts = []
    binds: Dict[str, Any] = {}
    for ti, tok in enumerate(tokens):
        tok_bind = f"fts{ti}"
        binds[tok_bind] = f"%{tok.upper()}%"
        col_checks = [f"INSTR(UPPER({col}), :{tok_bind}) > 0" for col in columns]
        parts.append("(" + " OR ".join(col_checks) + ")")
    where = " AND (" + " AND ".join(parts) + ")"
    return where, binds
