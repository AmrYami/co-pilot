from __future__ import annotations
import re
from typing import Any, Dict, List, Optional, Tuple
from datetime import date

from .contract_common import GROSS_SQL, OVERLAP_PRED, build_fts_clause, explain_window

# Dimension aliases we support for GROUP BY on Contract table
DIMENSIONS = {
    "stakeholder": "CONTRACT_STAKEHOLDER_1",
    "owner": "CONTRACT_OWNER",
    "owner_department": "OWNER_DEPARTMENT",
    "department_oul": "DEPARTMENT_OUL",
    "entity": "ENTITY",
    "entity_no": "ENTITY_NO",
}


RE_EQ_GENERIC = re.compile(
    r"(?i)\b([A-Z0-9_ ]+?)\s*=\s*(?:'([^']*)'|\"([^\"]*)\"|([^\s]+))"
)


def _norm_col(col: str) -> str:
    return col.strip().upper().replace(" ", "_")


def _extract_eq_filter(question: str) -> Optional[Dict[str, Any]]:
    match = RE_EQ_GENERIC.search(question or "")
    if not match:
        return None

    col = _norm_col(match.group(1))
    val = (match.group(2) or match.group(3) or match.group(4) or "").strip()
    if not col or not val:
        return None

    allowed = {"REQUEST_TYPE", "ENTITY_NO"}
    if col not in allowed:
        return None

    bind = "eq_0"
    predicate = f"UPPER(TRIM({col})) = UPPER(TRIM(:{bind}))"
    order = "REQUEST_DATE DESC" if col == "REQUEST_TYPE" else None
    return {"predicate": predicate, "binds": {bind: val}, "order": order, "col": col}


def _pick_measure(q: str) -> str:
    ql = (q or "").lower()
    if "gross" in ql:
        return GROSS_SQL
    # default: net contract value
    return "NVL(CONTRACT_VALUE_NET_OF_VAT,0)"


def _pick_window_strategy(q: str) -> str:
    """
    RULE:
      - If question explicitly mentions 'requested', use REQUEST_DATE window.
      - Otherwise, for generic 'contracts last X', use OVERLAP (START..END).
    """
    ql = (q or "").lower()
    if "request" in ql or "requested" in ql:
        return "REQUEST_DATE"
    return "OVERLAP"


def _build_window_pred(date_col: str) -> str:
    dc = date_col.upper()
    if dc == "REQUEST_DATE":
        return "REQUEST_DATE BETWEEN :date_start AND :date_end"
    if dc == "OVERLAP":
        return OVERLAP_PRED
    # Fallback to request_date
    return "REQUEST_DATE BETWEEN :date_start AND :date_end"


def _resolve_groupby(q: str) -> Optional[str]:
    ql = (q or "").lower()
    # heuristic for "by/per X"
    for key, col in DIMENSIONS.items():
        if f" by {key}" in ql or f" per {key}" in ql:
            return col
        # also support exact phrases common in your examples
        if key in ql and (" by " in ql or " per " in ql):
            return col
    if "stakeholder" in ql:
        return DIMENSIONS["stakeholder"]
    if "owner department" in ql or "department" in ql:
        return DIMENSIONS["owner_department"]
    if "department_oul" in ql:
        return DIMENSIONS["department_oul"]
    if "entity no" in ql:
        return DIMENSIONS["entity_no"]
    if "entity" in ql:
        return DIMENSIONS["entity"]
    return None


def plan_contract_query(
    q: str,
    *,
    explicit_dates: Optional[Tuple[date, date]],
    top_n: Optional[int],
    full_text_search: bool,
    fts_columns: List[str],
    fts_tokens: List[str],
) -> Tuple[str, Dict[str, Any], Dict[str, Any], str]:
    """
    Deterministic planner for Contract table queries.
    Returns: (sql, binds, meta, explain)
    """
    measure = _pick_measure(q)
    group_col = _resolve_groupby(q)
    wants_count = "(count" in (q or "").lower() or " count" in (q or "").lower()
    date_col = _pick_window_strategy(q)
    window_pred = _build_window_pred(date_col)
    explain_bits: List[str] = []

    binds: Dict[str, Any] = {}
    if explicit_dates:
        ds, de = explicit_dates
        binds["date_start"] = ds
        binds["date_end"] = de
        explain_bits.append(explain_window(date_col, ds, de))
    else:
        explain_bits.append("No explicit window; using default or none.")

    # FULL TEXT SEARCH
    fts_where, fts_binds = ("", {})
    if full_text_search and fts_columns and fts_tokens:
        fts_where, fts_binds = build_fts_clause(fts_columns, fts_tokens)
        binds.update(fts_binds)
        explain_bits.append(f"Applied full-text search over {len(fts_columns)} columns for tokens: {fts_tokens}.")

    # Patterns
    ql = (q or "").lower()
    sql: str

    eq_filter = _extract_eq_filter(q)
    if eq_filter:
        binds.update(eq_filter["binds"])

    if "expiring" in ql and wants_count:
        # Contracts expiring in X days (count) → COUNT on END_DATE window (inclusive)
        date_col = "END_DATE"
        explain_bits.append("Interpreting 'expiring' as END_DATE between window.")
        window_pred = "END_DATE BETWEEN :date_start AND :date_end"
        sql = f"SELECT COUNT(*) AS CNT FROM \"Contract\" WHERE {window_pred}{fts_where}"
        return sql, binds, {"group_by": None, "measure": "COUNT", "date_col": date_col}, " ".join(explain_bits)

    if group_col and not wants_count:
        where_parts: List[str] = []
        if explicit_dates:
            where_parts.append(window_pred)
        if eq_filter:
            where_parts.append(eq_filter["predicate"])

        sql_lines: List[str] = []
        if eq_filter:
            sql_lines.append(
                "SELECT\n"
                f"  {group_col} AS GROUP_KEY,\n"
                f"  SUM({GROSS_SQL}) AS TOTAL_GROSS,\n"
                "  COUNT(*) AS CNT\n"
                "FROM \"Contract\""
            )
        else:
            sql_lines.append(
                "SELECT\n"
                f"  {group_col} AS GROUP_KEY,\n"
                f"  SUM({measure}) AS MEASURE\n"
                "FROM \"Contract\""
            )

        if where_parts:
            sql_lines.append("WHERE " + " AND ".join(where_parts))
        if not where_parts and fts_where:
            sql_lines.append("WHERE 1=1")
        if fts_where:
            sql_lines[-1] = sql_lines[-1] + fts_where if sql_lines else "WHERE 1=1" + fts_where

        sql = "\n".join(sql_lines)
        sql += f"\nGROUP BY {group_col}"
        if eq_filter:
            sql += "\nORDER BY TOTAL_GROSS DESC"
            explain_bits.append(
                f"Aggregating gross and count by {group_col} with equality filter."
            )
            meta = {"group_by": group_col, "measure": "GROSS", "date_col": date_col}
        else:
            sql += "\nORDER BY MEASURE DESC"
            explain_bits.append(
                f"Aggregating by {group_col} and ordering by SUM(measure) DESC."
            )
            meta = {"group_by": group_col, "measure": "SUM", "date_col": date_col}

        if top_n:
            binds["top_n"] = int(top_n)
            sql += "\nFETCH FIRST :top_n ROWS ONLY"
        return sql, binds, meta, " ".join(explain_bits)

    if wants_count and not group_col:
        # Count by request window (or overlap if mentioned)
        if "status" in ql:
            # "Count of contracts by status" → grouped count
            sql = (
                "SELECT CONTRACT_STATUS AS GROUP_KEY, COUNT(*) AS CNT "
                "FROM \"Contract\" GROUP BY CONTRACT_STATUS ORDER BY CNT DESC"
            )
            explain_bits.append("Grouped count by CONTRACT_STATUS.")
            return sql, binds, {"group_by": "CONTRACT_STATUS", "measure": "COUNT"}, " ".join(explain_bits)
        # Else: simple count in window if exists, else all time
        if explicit_dates:
            sql = f"SELECT COUNT(*) AS CNT FROM \"Contract\" WHERE {window_pred}{fts_where}"
        else:
            sql = "SELECT COUNT(*) AS CNT FROM \"Contract\"" + fts_where
        explain_bits.append("Returning COUNT(*) without grouping.")
        return sql, binds, {"group_by": None, "measure": "COUNT", "date_col": date_col}, " ".join(explain_bits)

    # Top contracts (no group) by measure
    if "top" in ql and "contract" in ql:
        select_cols = "*"
        if explicit_dates:
            where_clause = f"WHERE {window_pred}{fts_where}"
        elif fts_where:
            where_clause = "WHERE 1=1" + fts_where
        else:
            where_clause = ""
        sql = (
            f"SELECT {select_cols} FROM \"Contract\"\n"
            f"{where_clause}\n"
            f"ORDER BY {measure} DESC"
        )
        if top_n:
            binds["top_n"] = int(top_n)
            sql += "\nFETCH FIRST :top_n ROWS ONLY"
        explain_bits.append("Top contracts by measure (descending).")
        return sql, binds, {"group_by": None, "measure": measure, "date_col": date_col}, " ".join(explain_bits)

    # Requested last X (explicit on REQUEST_DATE)
    if "requested" in ql:
        sql = (
            "SELECT * FROM \"Contract\"\n"
            "WHERE REQUEST_DATE BETWEEN :date_start AND :date_end"
            f"{fts_where}\nORDER BY REQUEST_DATE DESC"
        )
        explain_bits.append("Requested window detected; sorting by REQUEST_DATE DESC.")
        return sql, binds, {"date_col": "REQUEST_DATE"}, " ".join(explain_bits)

    # Specific filters:
    if "vat" in ql and ("null" in ql or "zero" in ql):
        # VAT null or zero and positive contract value
        pred = "(NVL(VAT, 0) = 0 AND NVL(CONTRACT_VALUE_NET_OF_VAT,0) > 0)"
        base = f"SELECT * FROM \"Contract\" WHERE {pred}"
        if explicit_dates:
            base += f" AND {window_pred}"
        base += fts_where + "\nORDER BY " + measure + " DESC"
        explain_bits.append("Applied VAT null/zero and value > 0 predicate.")
        return base, binds, {"filter": "vat_zero_or_null"}, " ".join(explain_bits)

    if "distinct entity" in ql or ("list" in ql and "entity" in ql and "count" in ql):
        sql = "SELECT ENTITY AS GROUP_KEY, COUNT(*) AS CNT FROM \"Contract\" GROUP BY ENTITY ORDER BY CNT DESC"
        explain_bits.append("Distinct ENTITY with counts.")
        return sql, binds, {"group_by": "ENTITY", "measure": "COUNT"}, " ".join(explain_bits)

    if eq_filter:
        where_parts = []
        if explicit_dates:
            where_parts.append(window_pred)
        where_parts.append(eq_filter["predicate"])
        sql = "SELECT * FROM \"Contract\""
        if where_parts:
            sql += "\nWHERE " + " AND ".join(where_parts)
        if not where_parts and fts_where:
            sql += "\nWHERE 1=1"
        if fts_where:
            sql += fts_where
        order = eq_filter.get("order") or "REQUEST_DATE DESC"
        sql += f"\nORDER BY {order}"
        explain_bits.append(f"Applied equality filter on {eq_filter['col']} from the question.")
        return sql, binds, {"group_by": None, "filter": eq_filter["col"], "date_col": date_col}, " ".join(explain_bits)

    # Fallback: list in window (if any) else all
    sql = "SELECT * FROM \"Contract\""
    if explicit_dates:
        sql += f"\nWHERE {window_pred}"
    sql += fts_where + "\nORDER BY REQUEST_DATE DESC"
    explain_bits.append("Fallback listing ordered by REQUEST_DATE DESC.")
    return sql, binds, {"fallback": True, "date_col": date_col}, " ".join(explain_bits)
