from __future__ import annotations

import json
import logging
import os
from functools import lru_cache
from typing import Any, Dict, List, Tuple

try:  # pragma: no cover - optional dependency in lean environments
    from sqlalchemy import create_engine, text
except Exception:  # pragma: no cover - fallback when SQLAlchemy missing
    create_engine = None  # type: ignore[assignment]
    text = None  # type: ignore[assignment]

try:  # pragma: no cover - optional dependency in lean environments
    from apps.dw.settings import get_setting, get_setting_json
except Exception:  # pragma: no cover - fallback when settings helpers unavailable
    def get_setting(*_args, **kwargs):  # type: ignore[return-type]
        return kwargs.get("default")

    def get_setting_json(*_args, **kwargs):  # type: ignore[return-type]
        return kwargs.get("default")


logger = logging.getLogger("dw.rate.sql")


def _wrap_with_rownum(sql: str, limit_param: str) -> str:
    # يناسب Oracle 11g وما قبل، وآمن على 12c+
    return f"SELECT * FROM ({sql}) WHERE ROWNUM <= :{limit_param}"


def _normalize_connections(raw: Any) -> List[Dict[str, str]]:
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except Exception:
            return []
    if not isinstance(raw, list):
        return []
    normalized: List[Dict[str, str]] = []
    for entry in raw:
        if not isinstance(entry, dict):
            continue
        name = str(entry.get("name") or "").strip()
        url = str(entry.get("url") or "").strip()
        if not url:
            continue
        normalized.append({"name": name, "url": url})
    return normalized


def _choose_url(connections: List[Dict[str, str]], datasource: str | None) -> str | None:
    if not connections:
        return None
    ds_norm = (datasource or "").strip().lower()
    if ds_norm:
        for entry in connections:
            entry_name = entry.get("name", "").strip().lower()
            if entry_name and entry_name == ds_norm:
                return entry["url"]
    if ds_norm:
        for entry in connections:
            entry_name = entry.get("name", "").strip().lower()
            if not entry_name and entry.get("url"):
                return entry["url"]
    return connections[0].get("url")


def _coerce_url(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        url = value.strip()
        return url or None
    return str(value)


@lru_cache(maxsize=4)
def _engine_from_url(url: str):
    if not url:
        raise RuntimeError("Database URL is empty")
    if create_engine is None:  # pragma: no cover - dependency guard
        raise RuntimeError("SQLAlchemy is required to create database engines")
    return create_engine(url, pool_pre_ping=True, future=True)


def get_engine_for_default_datasource():
    """Return a SQLAlchemy engine for the default DW datasource."""

    datasource = get_setting("DEFAULT_DATASOURCE", scope="namespace")
    if not datasource:
        datasource = get_setting("DEFAULT_DATASOURCE", scope="global")

    connections = _normalize_connections(
        get_setting_json("DB_CONNECTIONS", scope="namespace")
    )
    if not connections:
        connections = _normalize_connections(
            get_setting_json("DB_CONNECTIONS", scope="global")
        )

    url = _choose_url(connections, datasource)

    if not url:
        for scope in ("namespace", "global"):
            candidate = _coerce_url(get_setting("APP_DB_URL", scope=scope))
            if candidate:
                url = candidate
                break

    if not url:
        env_url = os.getenv("APP_DB_URL", "").strip()
        if env_url:
            url = env_url

    if not url:
        raise RuntimeError("No database URL configured for DW datasource execution")

    return _engine_from_url(url)


def execute_select(
    app_engine,
    sql: str,
    binds: Dict[str, Any],
    max_rows: int = 500,
) -> Tuple[List[str], List[List[Any]], int]:
    """
    ينفذ SELECT ويرجع (columns, rows_as_lists, row_count)
    - بيحط LIMIT رخيص بـROWNUM
    - بيحاول يجيب الأعمدة حتى لو مفيش صفوف
    """

    if text is None:  # pragma: no cover - dependency guard
        raise RuntimeError("SQLAlchemy is required to execute SELECT statements")

    binds_copy: Dict[str, Any] = dict(binds or {})
    base_limit = "_limit"
    limit_name = base_limit
    idx = 0
    while limit_name in binds_copy:
        idx += 1
        limit_name = f"{base_limit}_{idx}"
    limited_sql = _wrap_with_rownum(sql, limit_name)
    binds_copy[limit_name] = max_rows

    with app_engine.connect() as conn:
        res = conn.execute(text(limited_sql), binds_copy)
        rows = res.fetchall()
        columns = list(res.keys())
        row_count = len(rows)
        res.close()

        if row_count == 0 and not columns:
            probe_sql = _wrap_with_rownum(sql, limit_name)
            probe = conn.execute(text(probe_sql), {limit_name: 0})
            columns = list(probe.keys())
            probe.close()

        rows_list = [list(r) for r in rows]
        return columns, rows_list, row_count


__all__ = ["execute_select", "get_engine_for_default_datasource"]
