from __future__ import annotations

import csv
import os
import re
import threading
from dataclasses import dataclass
from io import StringIO
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

SAFE_SQL_RE = re.compile(r"(?is)^\s*(with|select)\b")

_ENGINES: Dict[str, Engine] = {}

_MEM_ENGINE: Engine | None = None
_MEM_URL: str | None = None
_MEM_LOCK = threading.Lock()


def get_engine_for_url(url: str, *, pool_pre_ping: bool = True, pool_recycle: int = 3600) -> Engine:
    """Create or reuse an Engine for the provided SQLAlchemy URL."""

    if not url:
        raise ValueError("Database URL must be provided")

    key = f"url::{url}::{pool_recycle}" if pool_pre_ping else f"url::{url}::np"
    if key in _ENGINES:
        return _ENGINES[key]

    engine = create_engine(url, pool_pre_ping=pool_pre_ping, pool_recycle=pool_recycle)
    _ENGINES[key] = engine
    return engine


@dataclass
class SQLExecutionResult:
    """Normalised result wrapper for SQL execution."""

    ok: bool
    columns: List[str]
    rows: List[Dict[str, Any]]
    rowcount: int
    error: Optional[str] = None

    def dict(self) -> Dict[str, Any]:
        return {
            "ok": self.ok,
            "columns": self.columns,
            "rows": self.rows,
            "rowcount": self.rowcount,
            "error": self.error,
        }


def get_app_engine(settings, namespace: str) -> Engine:
    url = settings.get_app_db_url(namespace=namespace)
    if not url:
        raise RuntimeError("APP_DB_URL not configured")
    key = f"{namespace}::{url}"
    if key in _ENGINES:
        return _ENGINES[key]
    eng = get_engine_for_url(url)
    _ENGINES[key] = eng
    return eng


def init_mem_engine(settings: Any) -> "Engine":
    """Create (or reuse) the global mem engine from settings/env."""
    global _MEM_ENGINE, _MEM_URL
    url = None
    if settings is not None and hasattr(settings, "get"):
        url = settings.get("MEMORY_DB_URL", scope="global")
    if not url:
        url = os.getenv("MEMORY_DB_URL")
    if not url:
        raise RuntimeError("MEMORY_DB_URL not set in settings or environment")

    if _MEM_ENGINE is not None and _MEM_URL == url:
        return _MEM_ENGINE

    with _MEM_LOCK:
        if _MEM_ENGINE is not None and _MEM_URL == url:
            return _MEM_ENGINE
        _MEM_ENGINE = create_engine(url, pool_pre_ping=True, future=True)
        _MEM_URL = url
        return _MEM_ENGINE


def get_mem_engine(settings: Any) -> "Engine":
    """Return the global memory engine, initialising it with provided settings when needed."""
    global _MEM_ENGINE
    if _MEM_ENGINE is not None:
        return _MEM_ENGINE
    if settings is None:
        raise RuntimeError("Settings must be provided to initialise the memory engine")
    return init_mem_engine(settings)

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


def run_sql(engine: Engine, sql: str, limit: Optional[int] = None) -> SQLExecutionResult:
    """Execute a read-only SQL statement and normalise the response."""

    valid, message = validate_select(sql)
    if not valid:
        return SQLExecutionResult(
            ok=False,
            columns=[],
            rows=[],
            rowcount=0,
            error=message,
        )

    try:
        result = run_select(engine, sql, limit)
    except Exception as exc:  # pragma: no cover - passthrough to caller
        return SQLExecutionResult(
            ok=False,
            columns=[],
            rows=[],
            rowcount=0,
            error=str(exc),
        )

    rows = result.get("rows", [])
    return SQLExecutionResult(
        ok=True,
        columns=result.get("columns", []),
        rows=rows,
        rowcount=result.get("rowcount") or len(rows),
    )

def as_csv(result: Dict[str, Any]) -> bytes:
    cols = result["columns"]
    rows = result["rows"]
    sio = StringIO()
    w = csv.writer(sio)
    w.writerow(cols)
    for r in rows:
        w.writerow([r.get(c) for c in cols])
    return sio.getvalue().encode("utf-8")
