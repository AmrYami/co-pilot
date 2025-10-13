from __future__ import annotations

from typing import Any, Dict, List, Tuple

try:  # pragma: no cover - optional dependency during tests
    from sqlalchemy import create_engine, inspect, text
except Exception:  # pragma: no cover - fallback when SQLAlchemy missing
    create_engine = None  # type: ignore[assignment]
    inspect = None  # type: ignore[assignment]
    text = None  # type: ignore[assignment]

try:  # pragma: no cover - optional dependency during tests
    from apps.dw.settings import get_setting
except Exception:  # pragma: no cover - fallback when settings helpers unavailable
    def get_setting(*_args, **kwargs):  # type: ignore[return-type]
        return kwargs.get("default")


def _get_engine():
    if create_engine is None:  # pragma: no cover - guard when dependency missing
        raise RuntimeError("SQLAlchemy is required to execute RATE queries")
    url = get_setting("APP_DB_URL", scope="namespace") or get_setting(
        "APP_DB_URL", scope="global"
    )
    return create_engine(url, pool_pre_ping=True, future=True)


def fetch_columns_fallback(table: str) -> List[str]:
    if create_engine is None or text is None:  # pragma: no cover - dependency guard
        return []
    eng = _get_engine()
    try:
        if inspect is not None:
            insp = inspect(eng)
            cols = [c["name"] for c in insp.get_columns(table)]
            if cols:
                return cols
    except Exception:
        pass
    try:
        with eng.connect() as conn:
            rs = conn.execute(text(f'SELECT * FROM "{table}" WHERE 1=0'))
            return list(rs.keys())
    except Exception:
        return []


def exec_sql_with_columns(
    sql: str, binds: Dict[str, Any], table: str
) -> Tuple[List[str], List[List[Any]]]:
    if create_engine is None or text is None:  # pragma: no cover - dependency guard
        raise RuntimeError("SQLAlchemy is required to execute RATE queries")
    eng = _get_engine()
    with eng.connect() as conn:
        rs = conn.execute(text(sql), binds or {})
        keys = list(rs.keys())
        rows = [list(row) for row in rs.fetchall()]
    if not keys:
        keys = fetch_columns_fallback(table)
    return keys, rows
