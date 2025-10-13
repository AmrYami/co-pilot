"""Shared SQL execution helpers used by DW endpoints."""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Sequence, Tuple

try:  # pragma: no cover - optional dependency in lean test environments
    from sqlalchemy import text
except Exception:  # pragma: no cover - fallback stub when SQLAlchemy absent
    def text(sql: str):  # type: ignore[return-type]
        return sql

try:  # pragma: no cover - primary engine factory used by production /dw/answer
    from apps.dw.db import get_engine  # type: ignore
except Exception:  # pragma: no cover - allow tests without full DB stack
    get_engine = None  # type: ignore[assignment]

try:  # pragma: no cover - lightweight fetch fallback used in stubs/tests
    from apps.dw.db import fetch_rows  # type: ignore
except Exception:  # pragma: no cover - optional helper absent outside tests
    fetch_rows = None  # type: ignore[assignment]

from apps.dw.settings import get_namespace_settings


def _normalize_row(row: Any, columns: Sequence[str]) -> List[Any]:
    if isinstance(row, dict):
        return [row.get(col) for col in columns]
    if isinstance(row, (list, tuple)):
        return list(row)
    return [row]


def _columns_from_iter(rows: Iterable[Any]) -> List[str]:
    for row in rows:
        if isinstance(row, dict):
            return list(row.keys())
        if hasattr(row, "keys") and callable(row.keys):
            return list(row.keys())  # type: ignore[call-arg]
    return []


def run_select_with_columns(
    sql: str, binds: Dict[str, Any] | None = None, datasource: str | None = None
) -> Tuple[List[str], List[List[Any]]]:
    """Execute ``sql`` and return ``(columns, rows)`` similar to ``/dw/answer``.

    The helper mirrors the behaviour of the production answer endpoint by relying on the
    datasource registry configured via ``DEFAULT_DATASOURCE``. When the SQLAlchemy stack
    or concrete datasource factory is unavailable (for example in unit tests) we fall
    back to the legacy ``fetch_rows`` stub so existing lightweight suites continue to
    run without additional dependencies.
    """

    effective_binds: Dict[str, Any] = dict(binds or {})
    ns_settings = get_namespace_settings("dw::common") or {}
    datasource_name = datasource or ns_settings.get("DEFAULT_DATASOURCE") or "docuware"

    engine = None
    if callable(get_engine):
        try:
            engine = get_engine(datasource_name)  # type: ignore[misc]
        except TypeError:
            engine = get_engine()  # type: ignore[call-arg]
        except Exception:
            engine = None

    if engine is None:
        if callable(fetch_rows):
            rows = fetch_rows(sql, effective_binds)
            columns = _columns_from_iter(rows)
            normalized = [_normalize_row(row, columns) for row in rows]
            return columns, normalized
        raise RuntimeError("Database engine not configured for DW datasource execution")

    with engine.connect() as conn:  # type: ignore[operator]
        result = conn.execute(text(sql), effective_binds)
        columns = list(result.keys())
        raw_rows = result.fetchall()

    rows = [_normalize_row(row, columns) for row in raw_rows]
    return columns, rows


__all__ = ["run_select_with_columns"]
