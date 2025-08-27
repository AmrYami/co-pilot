# core/sql_exec.py
from __future__ import annotations
from typing import Any, Dict, Optional, Tuple
from sqlalchemy import text
from sqlalchemy.engine import Engine
import re, csv
from io import StringIO

SAFE_SQL_RE = re.compile(r"(?is)^\s*(with|select)\b")

def validate_select(sql: str) -> Tuple[bool, str]:
    s = sql.strip().lstrip("(")
    if not SAFE_SQL_RE.match(s):
        return False, "Only SELECT/CTE queries are allowed"
    return True, ""

def explain(engine: Engine, sql: str) -> None:
    with engine.connect() as c:
        c.execute(text(f"EXPLAIN {sql}"))

def run_select(engine: Engine, sql: str, limit: Optional[int] = None) -> Dict[str, Any]:
    s = sql.strip().rstrip(";")
    if limit and " limit " not in s.lower():
        s = f"{s} LIMIT {int(limit)}"
    with engine.connect() as c:
        rs = c.execute(text(s))
        cols = list(rs.keys())
        rows = [dict(zip(cols, list(r))) for r in rs]
    return {"columns": cols, "rows": rows, "rowcount": len(rows)}

def as_csv(result: Dict[str, Any]) -> bytes:
    cols = result["columns"]
    rows = result["rows"]
    sio = StringIO()
    w = csv.writer(sio)
    w.writerow(cols)
    for r in rows:
        w.writerow([r.get(c) for c in cols])
    return sio.getvalue().encode("utf-8")
