from typing import Dict, Any, Optional, Tuple, List
import logging
import re
from datetime import date
from dateutil.relativedelta import relativedelta


LOGGER = logging.getLogger("dw.sql_builder")


# Helper to read optional strict overlap from Settings
def _bool_env(v) -> bool:
    if v is None:
        return False
    if isinstance(v, bool):
        return v
    return str(v).lower() in ("1", "true", "yes", "y", "on")


def _overlap_clause(strict: bool) -> str:
    """
    Overlap filter: active during [date_start, date_end]
       START_DATE <= end AND END_DATE >= start
    If strict: require both dates to be non-null.
    """
    if strict:
        return (
            "(START_DATE IS NOT NULL AND END_DATE IS NOT NULL "
            "AND START_DATE <= :date_end AND END_DATE >= :date_start)"
        )
    return "(START_DATE <= :date_end AND END_DATE >= :date_start)"


def _select_for_non_agg(*, wants_all: bool) -> str:
    # Your default is "select all" when not aggregated
    if wants_all:
        return "*"
    # If you prefer a light list when not aggregated, uncomment:
    # return "CONTRACT_ID, CONTRACT_OWNER, REQUEST_DATE, START_DATE, END_DATE"
    return "*"


def _gross_expr() -> str:
    return (
        "NVL(CONTRACT_VALUE_NET_OF_VAT,0) + "
        "CASE WHEN NVL(VAT,0) BETWEEN 0 AND 1 "
        "THEN NVL(CONTRACT_VALUE_NET_OF_VAT,0)*NVL(VAT,0) ELSE NVL(VAT,0) END"
    )


def build_fts_where(intent: Dict[str, Any], bind_prefix: str = "fts") -> Tuple[Optional[str], Dict[str, Any]]:
    if not intent.get("full_text_search"):
        return None, {}

    columns = intent.get("fts_columns") or []
    tokens = intent.get("fts_tokens") or []
    if not columns or not tokens:
        return None, {}

    def _quote(col: str) -> str:
        cleaned = col.strip()
        if cleaned.startswith('"') and cleaned.endswith('"'):
            return cleaned
        if re.fullmatch(r"[A-Z0-9_]+", cleaned):
            return f'"{cleaned}"'
        return cleaned

    binds: Dict[str, Any] = {}
    token_parts: List[str] = []
    for token in tokens:
        if not isinstance(token, str) or not token.strip():
            continue
        bind_key = f"{bind_prefix}_{len(binds)}"
        binds[bind_key] = f"%{token.strip()}%"
        col_predicates = [
            f"UPPER(TRIM({_quote(col)})) LIKE UPPER(:{bind_key})"
            for col in columns
            if isinstance(col, str) and col.strip()
        ]
        if col_predicates:
            token_parts.append("(" + " OR ".join(col_predicates) + ")")

    if not token_parts:
        return None, {}

    operator = (intent.get("fts_operator") or "OR").upper()
    joiner = " AND " if operator == "AND" else " OR "
    clause = "(" + joiner.join(token_parts) + ")"
    return clause, binds


def apply_order_by(sql: str, col: str, desc: bool) -> str:
    sql_no_ob = re.sub(r"\bORDER\s+BY\b.*$", "", sql, flags=re.IGNORECASE | re.DOTALL).rstrip()
    direction = "DESC" if desc else "ASC"
    return f"{sql_no_ob}\nORDER BY {col} {direction}"


def build_sql(intent: Dict[str, Any], settings, *, table: str = "Contract") -> Tuple[str, Dict[str, Any], Dict[str, Any]]:
    """
    Returns (sql, binds, meta)
    intent: dict from NLIntent (dict-like)
    settings: Settings reader (core.settings.Settings)
    """
    it = intent
    meta: Dict[str, Any] = {}
    binds: Dict[str, Any] = {}

    wants_all = bool(it.get("wants_all_columns", True))
    measure = it.get("measure_sql") or "NVL(CONTRACT_VALUE_NET_OF_VAT,0)"
    agg = (it.get("agg") or "").lower() or None
    group_by = it.get("group_by")
    top_n = it.get("top_n")
    sort_by = it.get("sort_by") or measure
    sort_desc = bool(it.get("sort_desc", True))

    # date window binds
    strict_overlap = _bool_env(settings.get("DW_OVERLAP_STRICT", 0))
    date_col = it.get("date_column") or "OVERLAP"
    exp_dates = it.get("explicit_dates")
    expire_days = it.get("expire")

    # Build WHERE and binds for time
    where_parts: List[str] = []
    if date_col == "OVERLAP":
        if exp_dates:
            binds["date_start"] = exp_dates["start"]
            binds["date_end"] = exp_dates["end"]
            where_parts.append(_overlap_clause(strict_overlap))
    elif date_col == "REQUEST_DATE":
        if exp_dates:
            binds["date_start"] = exp_dates["start"]
            binds["date_end"] = exp_dates["end"]
            where_parts.append("REQUEST_DATE BETWEEN :date_start AND :date_end")
    elif date_col == "END_DATE" and expire_days:
        # expiring in N days
        binds["date_start"] = exp_dates["start"]
        binds["date_end"] = exp_dates["end"]
        where_parts.append("END_DATE BETWEEN :date_start AND :date_end")
    elif exp_dates:
        # fallback: request_date
        binds["date_start"] = exp_dates["start"]
        binds["date_end"] = exp_dates["end"]
        where_parts.append("REQUEST_DATE BETWEEN :date_start AND :date_end")

    fts_clause, fts_binds = build_fts_where(it)
    if fts_clause:
        where_parts.append(fts_clause)
        binds.update(fts_binds)
        LOGGER.info(
            '[dw] {"fts": {"enabled": true, "tokens": %s, "columns": %s, "binds": %s}}',
            intent.get("fts_tokens", []),
            intent.get("fts_columns", []),
            list(fts_binds.keys()),
        )
    else:
        LOGGER.info(
            '[dw] {"fts": {"enabled": false, "tokens": %s, "columns": %s}}',
            intent.get("fts_tokens", []),
            intent.get("fts_columns", []),
        )

    # Special "by status (all time)" — detect quickly
    # handled by group_by==CONTRACT_STATUS + agg="count"
    # Nothing special here beyond the generic aggregator path.

    # Heuristics for specific questions that need deterministic SQL
    qtxt = (it.get("notes") or {}).get("q", "").lower()

    # Contracts where VAT is null or zero but contract value > 0.
    if "vat" in qtxt and "value" in qtxt and (("null" in qtxt and "zero" in qtxt) or "null or zero" in qtxt):
        sel = _select_for_non_agg(wants_all=wants_all)
        sql = (
            f"SELECT {sel} FROM \"{table}\"\n"
            f"WHERE NVL(VAT,0) = 0 AND NVL(CONTRACT_VALUE_NET_OF_VAT,0) > 0\n"
            f"ORDER BY NVL(CONTRACT_VALUE_NET_OF_VAT,0) DESC"
        )
        return sql, binds, {"pattern": "vat_zero_positive_value"}

    # Show contracts where REQUEST TYPE = Renewal in YEAR.
    if "renewal" in qtxt:
        sel = _select_for_non_agg(wants_all=wants_all)
        # Pull year from explicit_dates if provided by parser
        if exp_dates:
            sql = (
                f"SELECT {sel} FROM \"{table}\"\n"
                f"WHERE REQUEST_TYPE = 'Renewal' "
                f"AND REQUEST_DATE BETWEEN :date_start AND :date_end\n"
                f"ORDER BY REQUEST_DATE DESC"
            )
            return sql, binds, {"pattern": "renewal_year"}
        # fallback: just REQUEST_TYPE
        sql = (
            f"SELECT {sel} FROM \"{table}\"\n"
            f"WHERE REQUEST_TYPE = 'Renewal'\n"
            f"ORDER BY REQUEST_DATE DESC"
        )
        return sql, binds, {"pattern": "renewal_no_year"}

    # Distinct ENTITY values and their counts
    if "distinct" in qtxt and "entity" in qtxt and "count" in qtxt:
        sql = (
            f"SELECT ENTITY AS GROUP_KEY, COUNT(*) AS CNT\n"
            f"FROM \"{table}\"\n"
            f"GROUP BY ENTITY\n"
            f"ORDER BY CNT DESC"
        )
        return sql, binds, {"pattern": "entity_counts"}

    # Contracts missing CONTRACT_ID (data quality)
    if "missing" in qtxt and "contract_id" in qtxt:
        sel = _select_for_non_agg(wants_all=wants_all)
        sql = (
            f"SELECT {sel} FROM \"{table}\"\n"
            f"WHERE CONTRACT_ID IS NULL OR TRIM(CONTRACT_ID) = ''\n"
            f"ORDER BY REQUEST_DATE DESC"
        )
        return sql, binds, {"pattern": "missing_contract_id"}

    # "list contracts owner department" → if you want a list of departments:
    if "list contracts owner" in qtxt and "department" in qtxt:
        sql = (
            f"SELECT DISTINCT OWNER_DEPARTMENT\n"
            f"FROM \"{table}\"\n"
            f"ORDER BY OWNER_DEPARTMENT"
        )
        return sql, binds, {"pattern": "owner_dept_list"}

    # Monthly trend last 12 months by REQUEST_DATE (counts)
    if "monthly trend" in qtxt:
        # Require REQUEST_DATE window
        if not exp_dates:
            # default 12 months
            end = date.today().replace(day=1) + relativedelta(months=1) - relativedelta(days=1)
            start = end.replace(day=1) - relativedelta(months=11)
            binds["date_start"] = start.isoformat()
            binds["date_end"] = end.isoformat()
        sql = (
            f"SELECT TRUNC(REQUEST_DATE,'MM') AS MONTH_BUCKET, COUNT(*) AS CNT\n"
            f"FROM \"{table}\"\n"
            f"WHERE REQUEST_DATE BETWEEN :date_start AND :date_end\n"
            f"GROUP BY TRUNC(REQUEST_DATE,'MM')\n"
            f"ORDER BY MONTH_BUCKET"
        )
        return sql, binds, {"pattern": "monthly_trend_request_date"}

    # Count of contracts by status (all time)
    if group_by == "CONTRACT_STATUS" and (agg == "count" or "count of contracts by status" in qtxt):
        sql = (
            f"SELECT CONTRACT_STATUS AS GROUP_KEY, COUNT(*) AS CNT\n"
            f"FROM \"{table}\"\n"
            f"GROUP BY CONTRACT_STATUS\n"
            f"ORDER BY CNT DESC"
        )
        return sql, binds, {"pattern": "count_by_status"}

    # Stakeholder gross over 1..8 slots (union-all) + window
    if group_by == "CONTRACT_STAKEHOLDER_1" and "stakeholder" in qtxt:
        gross = _gross_expr() if "gross" in qtxt else measure
        subqs = []
        slots = int(settings.get("DW_STAKEHOLDER_SLOTS", 8) or 8)
        where_time = ""
        if date_col == "OVERLAP" and exp_dates:
            where_time = f"WHERE {_overlap_clause(strict_overlap)}"
        elif date_col == "REQUEST_DATE" and exp_dates:
            where_time = "WHERE REQUEST_DATE BETWEEN :date_start AND :date_end"
        elif date_col == "END_DATE" and exp_dates:
            where_time = "WHERE END_DATE BETWEEN :date_start AND :date_end"
        for i in range(1, slots + 1):
            subqs.append(f"SELECT CONTRACT_STAKEHOLDER_{i} AS STK, {gross} AS GVAL FROM \"{table}\" {where_time}")
        union = "\nUNION ALL\n".join(subqs)
        sql = (
            f"WITH U AS (\n{union}\n)\n"
            f"SELECT STK AS GROUP_KEY, SUM(GVAL) AS MEASURE\n"
            f"FROM U\nGROUP BY STK\nORDER BY MEASURE DESC"
        )
        if top_n:
            binds["top_n"] = top_n
            sql += "\nFETCH FIRST :top_n ROWS ONLY"
        return sql, binds, {"pattern": "stakeholder_union_all"}

    # Generic aggregated path (group_by present)
    if group_by:
        sel_dim = f"{group_by} AS GROUP_KEY"
        if agg == "count":
            sel_mea = "COUNT(*) AS CNT"
            order_col = "CNT"
            order_desc = True
        elif agg == "avg":
            sel_mea = f"AVG({measure}) AS MEASURE"
            order_col = "MEASURE"
            order_desc = True
        else:
            sel_mea = f"SUM({measure}) AS MEASURE"
            order_col = "MEASURE"
            order_desc = True
        where_expr = " AND ".join(where_parts)
        lines = [
            f"SELECT {sel_dim}, {sel_mea}",
            f"FROM \"{table}\"",
        ]
        if where_expr:
            lines.append(f"WHERE {where_expr}")
        lines.append(f"GROUP BY {group_by}")
        sql = "\n".join(lines)
        sql = apply_order_by(sql, order_col, order_desc)
        sql = _ensure_single_order_by(sql)
        if top_n:
            binds["top_n"] = top_n
            sql += "\nFETCH FIRST :top_n ROWS ONLY"
        return sql, binds, {"pattern": "generic_agg"}

    # Non-aggregated (top contracts by value, overlap or request_date)
    sel = _select_for_non_agg(wants_all=wants_all)
    where_expr = " AND ".join(where_parts)
    lines = [f"SELECT {sel} FROM \"{table}\""]
    if where_expr:
        lines.append(f"WHERE {where_expr}")
    sql = "\n".join(lines)
    eq_filters_present = bool(it.get("eq_filters"))

    if sort_by:
        order_col = sort_by
        order_desc = sort_desc
    elif it.get("user_requested_top_n"):
        order_col = measure
        order_desc = True
    elif eq_filters_present:
        order_col = "REQUEST_DATE"
        order_desc = True
    else:
        order_col = measure
        order_desc = sort_desc
    sql = apply_order_by(sql, order_col, order_desc)
    sql = _ensure_single_order_by(sql)
    if top_n:
        binds["top_n"] = top_n
        sql += "\nFETCH FIRST :top_n ROWS ONLY"
    return sql, binds, {"pattern": "generic_non_agg"}


def _ensure_single_order_by(sql: str) -> str:
    lines = [ln for ln in sql.splitlines() if ln.strip()]
    order_indices = [i for i, ln in enumerate(lines) if ln.strip().upper().startswith("ORDER BY ")]
    if len(order_indices) <= 1:
        return "\n".join(lines)
    last = order_indices[-1]
    kept: List[str] = []
    for idx, line in enumerate(lines):
        if idx in order_indices and idx != last:
            continue
        kept.append(line)
    return "\n".join(kept)
