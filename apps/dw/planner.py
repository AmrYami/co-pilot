from __future__ import annotations
from typing import Any, Dict, List, Optional, Tuple

from apps.dw.contracts.builder import build_contracts_sql
from apps.dw.contracts.builder_contracts import (
    GROSS_EXPR,
    build_grouped_gross_per_dim,
    build_owner_vs_oul_diff,
    build_top_contracts_by_gross,
    build_top_contracts_by_net,
    build_yoy_overlap,
)

# Re-exported so legacy callers can import from planner if needed.
_ = GROSS_EXPR


def _is_contracts_namespace(namespace: str) -> bool:
    return namespace.startswith("dw::")


def _route_contract_intent(intent: Dict[str, Any]) -> Tuple[Optional[str], Dict[str, Any], str]:
    """Build SQL for Contract questions. Comments in English only."""

    q = (intent.get("notes", {}) or {}).get("q", "") or intent.get("q", "")
    q = str(q or "")
    top_n = intent.get("top_n")
    ds = ":date_start"
    de = ":date_end"
    # Switches:
    use_window = bool(intent.get("has_time_window"))
    q_lower = q.lower()
    wants_gross = bool(intent.get("wants_gross") or ("gross" in q_lower))
    group_dim = intent.get("group_by")

    # Top/Bottom by NET or GROSS:
    if top_n and not group_dim and "contract" in q_lower:
        if wants_gross:
            sql, extra_binds, explain = build_top_contracts_by_gross(q, use_window, ":top_n", ds, de)
        else:
            sql, extra_binds, explain = build_top_contracts_by_net(q, use_window, ":top_n", ds, de)
        return sql, extra_binds, explain

    # Per-dimension gross (OWNER_DEPARTMENT / DEPARTMENT_OUL / ENTITY / ENTITY_NO / CONTRACT_STATUS / REQUEST_TYPE):
    if group_dim in {"OWNER_DEPARTMENT", "DEPARTMENT_OUL", "ENTITY", "ENTITY_NO", "CONTRACT_STATUS", "REQUEST_TYPE"}:
        agg = intent.get("agg") or "SUM"
        sql, extra_binds, explain = build_grouped_gross_per_dim(group_dim, use_window, ds, de, agg=agg)
        return sql, extra_binds, explain

    # Owner vs OUL comparison:
    if "owner department" in q_lower and "oul" in q_lower:
        return build_owner_vs_oul_diff()

    # Year-over-year comparison:
    if "year-over-year" in q_lower or "yoy" in q_lower:
        # Binds expected: :ds, :de, :p_ds, :p_de (built by date parser)
        sql, extra_binds, explain = build_yoy_overlap(":ds", ":de", ":p_ds", ":p_de")
        return sql, extra_binds, explain

    # Fallback to previous deterministic logic:
    return None, {}, ""


def build_sql(
    question: str,
    intent: Dict,
    *,
    table: str = "Contract",
    fts_columns: Optional[List[str]] = None,
) -> Tuple[str, Dict[str, object]]:
    """Thin planner facade used by tests and admin routes."""

    namespace = str(intent.get("namespace") or "dw::common")
    if _is_contracts_namespace(namespace):
        sql, extra_binds, _ = _route_contract_intent(intent)
        if sql:
            binds = dict(intent.get("binds") or {})
            if intent.get("top_n") is not None and "top_n" not in binds:
                binds["top_n"] = intent["top_n"]
            binds.update(extra_binds or {})
            return sql, binds

    return build_contracts_sql(intent, table=table, fts_columns=fts_columns)
