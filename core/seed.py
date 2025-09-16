"""Utilities for seeding metrics, mappings, and snippets into the memory store."""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Sequence

from sqlalchemy import text
from sqlalchemy.engine import Engine


@dataclass
class UpsertResult:
    """Simple container capturing how many records were written."""

    count: int


def _json_or_null(value: Any) -> str:
    """Return a JSON-serialised representation suitable for CAST(:param AS jsonb)."""

    if value is None:
        return "null"
    return json.dumps(value)


def upsert_metrics(
    mem_engine: Engine,
    *,
    namespace: str,
    metrics: Sequence[Mapping[str, Any]] | None,
) -> UpsertResult:
    """Insert or replace metric definitions for the provided namespace."""

    items = list(metrics or [])
    if not items:
        return UpsertResult(count=0)

    delete_sql = text(
        "DELETE FROM mem_metrics WHERE namespace = :ns AND metric_key = :metric_key"
    )
    insert_sql = text(
        """
        INSERT INTO mem_metrics(
            namespace,
            metric_key,
            metric_name,
            description,
            calculation_sql,
            required_tables,
            required_columns,
            category,
            is_active,
            created_at,
            updated_at
        )
        VALUES (
            :namespace,
            :metric_key,
            :metric_name,
            :description,
            :calculation_sql,
            CAST(:required_tables AS jsonb),
            CAST(:required_columns AS jsonb),
            :category,
            TRUE,
            NOW(),
            NOW()
        )
        """
    )

    count = 0
    with mem_engine.begin() as conn:
        for metric in items:
            metric_key = metric.get("metric_key")
            if not metric_key:
                continue
            conn.execute(delete_sql, {"ns": namespace, "metric_key": metric_key})
            payload = {
                "namespace": namespace,
                "metric_key": metric_key,
                "metric_name": metric.get("metric_name") or metric_key,
                "description": metric.get("description"),
                "calculation_sql": metric.get("calculation_sql"),
                "required_tables": _json_or_null(metric.get("required_tables")),
                "required_columns": _json_or_null(metric.get("required_columns")),
                "category": metric.get("category"),
            }
            conn.execute(insert_sql, payload)
            count += 1
    return UpsertResult(count=count)


def upsert_mappings(
    mem_engine: Engine,
    *,
    namespace: str,
    mappings: Sequence[Mapping[str, Any]] | None,
) -> UpsertResult:
    """Insert or replace user-to-canonical term mappings."""

    items = list(mappings or [])
    if not items:
        return UpsertResult(count=0)

    delete_sql = text(
        """
        DELETE FROM mem_mappings
         WHERE namespace = :ns
           AND alias = :alias
           AND COALESCE(scope, '') = COALESCE(:scope, '')
        """
    )
    insert_sql = text(
        """
        INSERT INTO mem_mappings(
            namespace,
            alias,
            canonical,
            mapping_type,
            scope,
            is_active,
            created_at,
            updated_at
        )
        VALUES (
            :namespace,
            :alias,
            :canonical,
            :mapping_type,
            :scope,
            TRUE,
            NOW(),
            NOW()
        )
        """
    )

    count = 0
    with mem_engine.begin() as conn:
        for mapping in items:
            alias = mapping.get("alias")
            canonical = mapping.get("canonical")
            if not alias or not canonical:
                continue
            scope = mapping.get("scope")
            conn.execute(delete_sql, {"ns": namespace, "alias": alias, "scope": scope})
            payload = {
                "namespace": namespace,
                "alias": alias,
                "canonical": canonical,
                "mapping_type": mapping.get("mapping_type"),
                "scope": scope,
            }
            conn.execute(insert_sql, payload)
            count += 1
    return UpsertResult(count=count)


def upsert_snippet(
    mem_engine: Engine,
    *,
    namespace: str,
    title: str,
    sql_raw: str,
    input_tables: Iterable[str] | None = None,
    description: str | None = None,
    tags: Sequence[str] | None = None,
) -> None:
    """Replace (namespace, title) snippet with provided SQL."""

    delete_sql = text(
        "DELETE FROM mem_snippets WHERE namespace = :ns AND title = :title"
    )
    insert_sql = text(
        """
        INSERT INTO mem_snippets(
            namespace,
            title,
            description,
            sql_template,
            sql_raw,
            input_tables,
            output_columns,
            filters_applied,
            parameters,
            doc_md,
            doc_erd,
            tags,
            created_at,
            updated_at
        )
        VALUES (
            :namespace,
            :title,
            :description,
            :sql_raw,
            :sql_raw,
            CAST(:input_tables AS jsonb),
            CAST(:output_columns AS jsonb),
            CAST(:filters_applied AS jsonb),
            CAST(:parameters AS jsonb),
            :doc_md,
            :doc_erd,
            CAST(:tags AS jsonb),
            NOW(),
            NOW()
        )
        """
    )

    doc_md = f"### {title}\n\n````sql\n{sql_raw.strip()}\n````"
    payload = {
        "namespace": namespace,
        "title": title,
        "description": description or "Seeded snippet",
        "sql_raw": sql_raw.strip(),
        "input_tables": _json_or_null(list(input_tables or [])),
        "output_columns": "null",
        "filters_applied": "null",
        "parameters": "null",
        "doc_md": doc_md,
        "doc_erd": None,
        "tags": _json_or_null(list(tags or [])),
    }

    with mem_engine.begin() as conn:
        conn.execute(delete_sql, {"ns": namespace, "title": title})
        conn.execute(insert_sql, payload)
