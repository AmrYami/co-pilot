from __future__ import annotations
from typing import Iterable, Optional, Sequence
from sqlalchemy import text
import json
import datetime as dt

def build_doc_md(sql: str,
                 title: str | None = None,
                 rationale: str | None = None,
                 datasource: str | None = None) -> str:
    title = title or "Saved query"
    lines = [f"# {title}"]
    if datasource:
        lines.append(f"- **Datasource**: `{datasource}`")
    if rationale:
        lines.append(f"- **Rationale**: {rationale}")
    lines += ["", "```sql", sql.strip(), "```"]
    return "\n".join(lines)

def persist_snippet(mem_engine,
                    namespace: str,
                    sql_raw: str,
                    *,
                    title: str | None = None,
                    description: str | None = None,
                    tags: Optional[Sequence[str]] = None,
                    input_tables: Optional[Sequence[str]] = None,
                    filters_applied: Optional[Sequence[str]] = None,
                    parameters: Optional[dict] = None,
                    doc_md: Optional[str] = None,
                    datasource: Optional[str] = None,
                    verified: bool = False,
                    verified_by: Optional[str] = None) -> int:
    """
    Inserts into mem_snippets and returns new snippet id.
    """
    with mem_engine.begin() as c:
        res = c.execute(
            text("""
                INSERT INTO mem_snippets(
                    namespace, title, description,
                    sql_template, sql_raw,
                    input_tables, filters_applied, parameters,
                    doc_md, tags, datasource, is_verified, verified_by, created_at, updated_at
                )
                VALUES (
                    :ns, :title, :desc,
                    :tpl, :raw,
                    :in_tabs, :filters, :params,
                    :doc_md, :tags, :ds, :ver, :ver_by, NOW(), NOW()
                )
                RETURNING id
            """),
            {
                "ns": namespace,
                "title": title,
                "desc": description,
                "tpl": None,  # reserved for future parameterization
                "raw": sql_raw,
                "in_tabs": json.dumps(list(input_tables or [])),
                "filters": json.dumps(list(filters_applied or [])),
                "params": json.dumps(parameters or {}),
                "doc_md": doc_md or build_doc_md(sql_raw, title, datasource=datasource),
                "tags": json.dumps(list(tags or [])),
                "ds": datasource,
                "ver": bool(verified),
                "ver_by": verified_by,
            },
        )
        new_id = res.scalar_one()
    return int(new_id)
