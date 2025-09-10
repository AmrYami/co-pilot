# core/sql_exec.py
from __future__ import annotations
from typing import Any, Dict, Optional, Tuple
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
import re, csv
from io import StringIO

SAFE_SQL_RE = re.compile(r"(?is)^\s*(with|select)\b")

_ENGINES: Dict[str, Engine] = {}


def get_app_engine(settings, namespace: str) -> Engine:
    url = settings.get_app_db_url(namespace=namespace)
    if not url:
        raise RuntimeError("APP_DB_URL not configured")
    key = f"{namespace}::{url}"
    if key in _ENGINES:
        return _ENGINES[key]
    eng = create_engine(url, pool_pre_ping=True, pool_recycle=3600)
    _ENGINES[key] = eng
    return eng


def get_mem_engine(mem) -> Engine:
    """Return a cached Engine for the given mem database."""
    if isinstance(mem, Engine):
        return mem
    if isinstance(mem, str):
        key = f"mem::{mem}"
        if key in _ENGINES:
            return _ENGINES[key]
        eng = create_engine(mem, pool_pre_ping=True, pool_recycle=3600)
        _ENGINES[key] = eng
        return eng
    raise RuntimeError("MEM_ENGINE not configured")

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
