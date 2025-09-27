from __future__ import annotations
from typing import Dict, Tuple, Optional, List
from apps.dw.contracts.builder import build_contracts_sql


def build_sql(question: str, intent: Dict, *, table: str = "Contract", fts_columns: Optional[List[str]] = None) -> Tuple[str, Dict[str, object]]:
    """
    Thin planner facade used by tests and admin routes.
    Delegates to the table-specific builder for Contract.
    """
    return build_contracts_sql(intent, table=table, fts_columns=fts_columns)
