from __future__ import annotations
from datetime import date, datetime
from typing import Dict, Tuple, Optional, List

# NOTE: Keep this module strictly table-specific (Contract).
#       Cross-table / DocuWare-generic helpers should live elsewhere.

_NET = "NVL(CONTRACT_VALUE_NET_OF_VAT,0)"
_GROSS = f"{_NET} + CASE WHEN NVL(VAT,0) BETWEEN 0 AND 1 THEN {_NET} * NVL(VAT,0) ELSE NVL(VAT,0) END"

def _to_date(d: object) -> date:
    if isinstance(d, date):
        return d
    if isinstance(d, datetime):
        return d.date()
    # Expecting ISO string
    return datetime.fromisoformat(str(d)).date()

def _overlap_pred(date_start_bind: str = ":date_start", date_end_bind: str = ":date_end") -> str:
    # Strict overlap: start <= end AND end >= start (both not null)
    return f"(START_DATE IS NOT NULL AND END_DATE IS NOT NULL AND START_DATE <= {date_end_bind} AND END_DATE >= {date_start_bind})"

def build_contracts_sql(
    intent: Dict,
    *,
    table: str = "Contract",
    fts_columns: Optional[List[str]] = None
) -> Tuple[str, Dict[str, object]]:
    """
    Build Oracle SQL for the Contract table based on a normalized intent dict.
    Returns (sql, binds).
    Expected intent fields (subset):
      - explicit_dates: {start, end} or None
      - date_column: 'REQUEST_DATE' | 'END_DATE' | 'OVERLAP' | None
      - group_by: a column or None
      - agg: 'count' | 'sum' | 'avg' | None (for grouped measures)
      - measure_sql: SQL expr string for measure (defaults to NET)
      - sort_by, sort_desc, top_n
      - full_text_search: bool, fts_tokens: [str]
    """
    q_parts: List[str] = []
    binds: Dict[str, object] = {}
    select_list = "*"

    # WHERE parts
    where_parts: List[str] = []

    # 1) Time window / expiry semantics
    explicit = intent.get("explicit_dates")
    date_col = (intent.get("date_column") or "").upper() if intent.get("date_column") else None
    if explicit:
        binds["date_start"] = _to_date(explicit["start"])
        binds["date_end"]   = _to_date(explicit["end"])
        if date_col == "REQUEST_DATE":
            where_parts.append("REQUEST_DATE BETWEEN :date_start AND :date_end")
        elif date_col == "END_DATE":
            where_parts.append("END_DATE BETWEEN :date_start AND :date_end")
        elif date_col == "START_DATE":
            where_parts.append("START_DATE BETWEEN :date_start AND :date_end")
        elif date_col == "OVERLAP" or date_col is None:
            where_parts.append(_overlap_pred())
        else:
            # Fallback: safe overlap
            where_parts.append(_overlap_pred())

    # 2) Full-text-like filtering over configured columns (simple LIKE ORs)
    if intent.get("full_text_search") and intent.get("fts_tokens") and fts_columns:
        like_terms = []
        k = 0
        for tok in intent["fts_tokens"]:
            k += 1
            kb = f"kw{k}"
            binds[kb] = f"%{tok}%"
            ors = [f"UPPER({col}) LIKE UPPER(:{kb})" for col in fts_columns]
            like_terms.append("(" + " OR ".join(ors) + ")")
        if like_terms:
            where_parts.append("(" + " AND ".join(like_terms) + ")")

    # 3) Direct column filter (e.g., CONTRACT_STATUS = 'EXPIRE')
    #    Expect intent["direct_filter"] like {"column":"CONTRACT_STATUS","op":"=","value":"expire"}
    df = intent.get("direct_filter")
    if df and df.get("column"):
        col = df["column"]
        op  = df.get("op", "=").upper()
        val = df.get("value")
        if val is not None:
            binds["df_val"] = val
            where_parts.append(f"UPPER({col}) {op} UPPER(:df_val)")

    # 4) SELECT list and GROUP BY / measure
    group_by = intent.get("group_by")
    agg = intent.get("agg")
    measure_sql = (intent.get("measure_sql") or _NET)

    order_by: Optional[str] = None
    desc = bool(intent.get("sort_desc"))

    if group_by:
        # GROUPED output
        alias_measure = "MEASURE"
        if agg == "count":
            measure_expr = "COUNT(*)"
        elif agg == "avg":
            measure_expr = f"AVG({measure_sql})"
        elif agg == "sum" or agg is None:
            measure_expr = f"SUM({measure_sql})"
        else:
            measure_expr = f"SUM({measure_sql})"
        select_list = f"{group_by} AS GROUP_KEY, {measure_expr} AS {alias_measure}"
        order_by = alias_measure
    else:
        # ROW-LEVEL output (SELECT *)
        # Nothing special; ordering will be by sort_by if provided.
        order_by = intent.get("sort_by") or None

    # 5) Build SQL
    q_parts.append(f'SELECT {select_list} FROM "{table}"')
    if where_parts:
        q_parts.append("WHERE " + " AND ".join(where_parts))

    if order_by:
        q_parts.append(f"ORDER BY {order_by} {'DESC' if desc else 'ASC'}")

    # 6) Top-N
    if intent.get("top_n"):
        q_parts.append("FETCH FIRST :top_n ROWS ONLY")
        binds["top_n"] = int(intent["top_n"])

    sql = "\n".join(q_parts)
    return sql, binds
