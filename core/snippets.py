from __future__ import annotations

import re
from sqlalchemy import text
from typing import List, Dict, Any, Optional

_TABLE_RE = re.compile(r'\b(?:FROM|JOIN)\s+`?([a-zA-Z0-9_\.]+)`?', re.IGNORECASE)


def _extract_tables(sql: str) -> List[str]:
    found = []
    for m in _TABLE_RE.finditer(sql or ""):
        t = m.group(1)
        if t and t not in found:
            found.append(t)
    return found


def save_snippet(mem_engine, namespace: str, question: str, sql: str, tags: List[str] | None = None):
    if not sql:
        return
    input_tables = _extract_tables(sql)
    doc_md = f"""### Auto snippet
Source question: {question}

**Tables**: {", ".join(input_tables) or "-"}
"""

    payload = {
        "namespace": namespace,
        "title": (question[:120] if question else "Auto snippet"),
        "description": "Auto-saved after successful run",
        "sql_template": sql,
        "sql_raw": sql,
        "input_tables": input_tables,
        "output_columns": None,
        "filters_applied": None,
        "parameters": None,
        "doc_md": doc_md,
        "doc_erd": None,
        "tags": (tags or ["fa", "auto", "snippet"])
    }

    with mem_engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO mem_snippets(
                namespace, title, description, sql_template, sql_raw,
                input_tables, output_columns, filters_applied, parameters,
                doc_md, doc_erd, tags, created_at, updated_at
            )
            VALUES (
                :namespace, :title, :description, :sql_template, :sql_raw,
                CAST(:input_tables AS jsonb), CAST(:output_columns AS jsonb),
                CAST(:filters_applied AS jsonb), CAST(:parameters AS jsonb),
                :doc_md, :doc_erd, CAST(:tags AS jsonb), NOW(), NOW()
            )
        """), payload)


def autosave_snippet(
    mem_engine, namespace: str, datasource: Optional[str], sql_raw: str, tags: Optional[List[str]] = None
):
    """Store a minimal reusable snippet of a verified answer."""
    tags = tags or []
    with mem_engine.begin() as c:
        c.execute(
            text(
                """
            INSERT INTO mem_snippets(namespace, sql_raw, tags, datasource, created_at, updated_at)
            VALUES (:ns, :sql_raw, :tags::jsonb, :ds, NOW(), NOW())
            """
            ),
            {"ns": namespace, "sql_raw": sql_raw, "tags": tags, "ds": datasource},
        )

