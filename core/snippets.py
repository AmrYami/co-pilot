from __future__ import annotations
from typing import List, Dict, Any, Optional
from sqlalchemy import text
import json
from datetime import datetime

def save_snippet(mem_engine, namespace: str, sql_raw: str,
                 input_tables: List[str], filters_applied: List[str],
                 tags: List[str], datasource: Optional[str],
                 doc_md: Optional[str] = None, title: Optional[str] = None,
                 description: Optional[str] = None) -> int:
    with mem_engine.begin() as c:
        r = c.execute(text("""
            INSERT INTO mem_snippets(namespace, title, description, sql_template, sql_raw,
                                     input_tables, filters_applied, doc_md, tags, datasource,
                                     created_at, updated_at)
            VALUES (:ns, :title, :desc, :tmpl, :raw, :it, :filters, :doc, :tags, :ds, NOW(), NOW())
            RETURNING id
        """), {
            "ns": namespace,
            "title": title, "desc": description,
            "tmpl": sql_raw,  # keep same for now
            "raw": sql_raw,
            "it": json.dumps(input_tables or []),
            "filters": json.dumps(filters_applied or []),
            "doc": doc_md or "",
            "tags": json.dumps(tags or []),
            "ds": datasource
        })
        return r.scalar_one()
