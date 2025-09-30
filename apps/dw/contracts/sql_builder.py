# -*- coding: utf-8 -*-
"""
SQL builder for Contract.
All comments and strings inside code are in English only.
"""

from typing import Dict, Any, List, Tuple

from .enums import load_enum_synonyms, build_enum_where_clause


def build_where_from_filters(settings_get, filters: List[Dict[str, Any]]) -> Tuple[List[str], Dict[str, Any]]:
    """
    Translate structured filters to SQL fragments + binds.
    """
    where: List[str] = []
    binds: Dict[str, Any] = {}

    if not filters:
        return where, binds

    # Only REQUEST_TYPE for now; can be extended for other enum columns.
    syns = load_enum_synonyms(settings_get, table="Contract", column="REQUEST_TYPE")

    for idx, f in enumerate(filters):
        col = f.get("column") if isinstance(f, dict) else None
        if not col:
            continue
        if (f.get("kind") if isinstance(f, dict) else None) == "enum" and col.upper() == "REQUEST_TYPE":
            frag, b = build_enum_where_clause("REQUEST_TYPE", f.get("value", ""), syns, bind_prefix=f"rt_{idx}")
            if frag:
                where.append(frag)
            binds.update(b)
        # (Placeholders for future: owner department exact match, etc.)

    return where, binds


def attach_where_clause(base_sql: str, extra_where: List[str]) -> str:
    if not extra_where:
        return base_sql
    glue = " AND ".join([f"({w})" for w in extra_where if w])
    if not glue:
        return base_sql
    if " WHERE " in base_sql.upper():
        return f"{base_sql}\nAND {glue}"
    else:
        return f"{base_sql}\nWHERE {glue}"
