from __future__ import annotations
import re
from datetime import date, datetime
from typing import Dict, Tuple, Optional, List

# NOTE: Keep this module strictly table-specific (Contract).
#       Cross-table / DocuWare-generic helpers should live elsewhere.

_NET = "NVL(CONTRACT_VALUE_NET_OF_VAT,0)"


def gross_expr() -> str:
    return (
        "NVL(CONTRACT_VALUE_NET_OF_VAT,0) + CASE WHEN NVL(VAT,0) BETWEEN 0 AND 1 "
        "THEN NVL(CONTRACT_VALUE_NET_OF_VAT,0) * NVL(VAT,0) ELSE NVL(VAT,0) END"
    )


# --- Helpers: measures / overlap predicate ---
GROSS_EXPR = gross_expr()


def overlap_pred() -> str:
    return _overlap_pred()


# --- Case (15): missing CONTRACT_ID ---
def sql_missing_contract_id() -> str:
    return (
        'SELECT * FROM "Contract"\n'
        "WHERE CONTRACT_ID IS NULL OR TRIM(CONTRACT_ID) = ''\n"
        "ORDER BY REQUEST_DATE DESC"
    )


# --- Case (17): YTD top N by gross ---
def sql_ytd_top_gross() -> str:
    return (
        'SELECT * FROM "Contract"\n'
        f"WHERE {overlap_pred()}\n"
        f"ORDER BY {gross_expr()} DESC\n"
        "FETCH FIRST :top_n ROWS ONLY"
    )


# --- Case (32): YoY gross using overlap, two blocks ---
def sql_yoy_same_period_overlap() -> str:
    return (
        "SELECT 'CURRENT' AS PERIOD, SUM(" + GROSS_EXPR + ") AS TOTAL_GROSS\n"
        'FROM "Contract"\n'
        "WHERE (START_DATE IS NOT NULL AND END_DATE IS NOT NULL "
        "AND START_DATE <= :de AND END_DATE >= :ds)\n"
        "UNION ALL\n"
        "SELECT 'PREVIOUS' AS PERIOD, SUM(" + GROSS_EXPR + ") AS TOTAL_GROSS\n"
        'FROM "Contract"\n'
        "WHERE (START_DATE IS NOT NULL AND END_DATE IS NOT NULL "
        "AND START_DATE <= :p_de AND END_DATE >= :p_ds)"
    )


# --- Case (35): mismatches OWNER_DEPARTMENT vs DEPARTMENT_OUL ---
def sql_owner_vs_oul_mismatch() -> str:
    return (
        'SELECT OWNER_DEPARTMENT, DEPARTMENT_OUL, COUNT(*) AS CNT\n'
        'FROM "Contract"\n'
        "WHERE DEPARTMENT_OUL IS NOT NULL\n"
        "  AND TRIM(DEPARTMENT_OUL) <> ''\n"
        "  AND NVL(TRIM(OWNER_DEPARTMENT),'(None)') <> NVL(TRIM(DEPARTMENT_OUL),'(None)')\n"
        "GROUP BY OWNER_DEPARTMENT, DEPARTMENT_OUL\n"
        "ORDER BY CNT DESC"
    )

def _as_date(obj: object) -> date:
    if isinstance(obj, datetime):
        return obj.date()
    if isinstance(obj, date):
        return obj
    return date.fromisoformat(str(obj)[:10])


def _ensure_date_binds(binds: Dict[str, object], *keys: str) -> None:
    for key in keys:
        if key in binds and binds[key] is not None:
            binds[key] = _as_date(binds[key])

def _overlap_pred(date_start_bind: str = ":date_start", date_end_bind: str = ":date_end") -> str:
    # Strict overlap: start <= end AND end >= start (both not null)
    return (
        "(START_DATE IS NOT NULL AND END_DATE IS NOT NULL "
        f"AND START_DATE <= {date_end_bind} AND END_DATE >= {date_start_bind})"
    )

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
    q_norm = str(
        intent.get("raw_question_norm")
        or intent.get("raw_question")
        or (intent.get("notes") or {}).get("q")
        or ""
    ).strip().lower()

    # Special deterministic cases mapped by the parser or fallback keyword match.
    if "missing contract_id" in q_norm or "data quality" in q_norm:
        return sql_missing_contract_id(), {}

    if "ytd" in q_norm and "gross" in q_norm and "top" in q_norm:
        today_obj = (
            intent.get("today")
            or (intent.get("notes") or {}).get("today")
            or date.today()
        )
        today_date = _as_date(today_obj)
        year_match = re.search(r"\b(20\d{2})\b", q_norm)
        year = int(year_match.group(1)) if year_match else today_date.year
        top_hint = re.search(r"top\s+(\d+)", q_norm)
        default_top = int(top_hint.group(1)) if top_hint else 5
        try:
            top_n = int(intent.get("top_n", default_top))
        except (TypeError, ValueError):
            top_n = default_top
        binds = {
            "date_start": date(year, 1, 1),
            "date_end": today_date,
            "top_n": top_n,
        }
        _ensure_date_binds(binds, "date_start", "date_end")
        return sql_ytd_top_gross(), binds

    if "year-over-year" in q_norm or "yoy" in q_norm:
        today_obj = (
            intent.get("today")
            or (intent.get("notes") or {}).get("today")
            or date.today()
        )
        today_date = _as_date(today_obj)
        explicit = intent.get("explicit_dates") or {}
        ds = explicit.get("ds") or explicit.get("start") or intent.get("ds")
        de = explicit.get("de") or explicit.get("end") or intent.get("de")
        p_ds = explicit.get("p_ds") or explicit.get("previous_ds") or intent.get("p_ds")
        p_de = explicit.get("p_de") or explicit.get("previous_de") or intent.get("p_de")
        if not all([ds, de, p_ds, p_de]):
            this_year = today_date.year
            ds = ds or date(this_year, 1, 1)
            de = de or date(this_year, 3, 31)
            p_ds = p_ds or date(this_year - 1, 1, 1)
            p_de = p_de or date(this_year - 1, 3, 31)
        binds = {"ds": ds, "de": de, "p_ds": p_ds, "p_de": p_de}
        _ensure_date_binds(binds, "ds", "de", "p_ds", "p_de")
        return sql_yoy_same_period_overlap(), binds

    if "owner_department vs department_oul" in q_norm or (
        "department_oul" in q_norm and "owner" in q_norm
    ):
        return sql_owner_vs_oul_mismatch(), {}

    q_parts: List[str] = []
    binds: Dict[str, object] = {}
    select_list = "*"

    # WHERE parts
    where_parts: List[str] = []

    # 1) Time window / expiry semantics
    explicit = intent.get("explicit_dates")
    date_col = (intent.get("date_column") or "").upper() if intent.get("date_column") else None
    if explicit:
        binds["date_start"] = _as_date(explicit["start"])
        binds["date_end"] = _as_date(explicit["end"])
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
