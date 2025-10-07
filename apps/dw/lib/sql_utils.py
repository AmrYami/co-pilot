# -*- coding: utf-8 -*-
from typing import Dict, List


def gross_expr() -> str:
    return (
        "NVL(CONTRACT_VALUE_NET_OF_VAT,0) + CASE WHEN NVL(VAT,0) BETWEEN 0 AND 1 "
        "THEN NVL(CONTRACT_VALUE_NET_OF_VAT,0) * NVL(VAT,0) ELSE NVL(VAT,0) END"
    )


def merge_where(parts: List[str]) -> str:
    parts = [p.strip() for p in parts if p and p.strip()]
    if not parts:
        return ""
    return "WHERE " + " AND ".join(f"({p})" if not p.startswith("(") else p for p in parts)


def order_by_safe(existing_sql: str, order_clause: str) -> str:
    """
    Prevent duplicate ORDER BY: if existing_sql already has ORDER BY, remove it first.
    """
    sql = existing_sql
    low = sql.lower()
    idx = low.rfind(" order by ")
    if idx >= 0:
        sql = sql[:idx]
    if order_clause:
        sql = sql.rstrip() + "\n" + order_clause
    return sql


def direction_from_words(question: str, fallback: str = "DESC") -> str:
    q = (question or "").lower()
    if any(w in q for w in ["lowest", "bottom", "smallest", "cheapest", "اقل", "أقل"]):
        return "ASC"
    if any(w in q for w in ["highest", "top", "biggest", "largest"]):
        return "DESC"
    return fallback
