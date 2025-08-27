# apps/fa/learn.py
from __future__ import annotations
import re
from typing import Any, Dict, List, Tuple
from sqlalchemy import text
from sqlalchemy.engine import Engine

REV_RE = re.compile(r"\brevenue\b", re.I)
TYPE10_RE = re.compile(r"type\s*=?\s*10\b")
OVAMT_RE = re.compile(r"ov_amount\s*\+\s*ov_gst", re.I)

def learn_from_reply(mem: Engine, namespace: str, admin_text: str, final_sql: str | None) -> None:
    """
    Very small heuristics to persist obvious signals. Safe no-throw.
    """
    try:
        with mem.begin() as con:
            if REV_RE.search(admin_text) and (OVAMT_RE.search(admin_text) or (final_sql and OVAMT_RE.search(final_sql))):
                # 1) alias mapping revenue -> sales_amount
                con.execute(text("""
                    INSERT INTO mem_mappings(namespace, alias, canonical, mapping_type, scope, source, confidence, created_at, updated_at)
                    VALUES (:ns, :alias, :canon, 'term', 'global', 'admin', 0.95, NOW(), NOW())
                    ON CONFLICT (namespace, alias, mapping_type, scope) DO NOTHING
                """), {"ns": namespace, "alias": "revenue", "canon": "sales_amount"})
                # 2) rule for type=10 if hinted
                if TYPE10_RE.search(admin_text) or (final_sql and TYPE10_RE.search(final_sql)):
                    con.execute(text("""
                        INSERT INTO mem_rules(namespace, rule_name, rule_type, scope, condition_sql, description, priority, is_mandatory, source, confidence, created_at, updated_at)
                        VALUES (:ns, 'sales_is_type10', 'filter', 'debtor_trans', 'dt.type = 10', 'Invoices are type=10', 100, false, 'admin', 0.9, NOW(), NOW())
                        ON CONFLICT (namespace, rule_name) DO NOTHING
                    """), {"ns": namespace})
    except Exception:
        pass
