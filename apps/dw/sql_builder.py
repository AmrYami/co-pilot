from typing import Dict, Any, Iterable, Optional, Sequence, Tuple, List
import logging
import re
from datetime import date
from dateutil.relativedelta import relativedelta

from apps.dw.patchlib.settings_util import get_fts_engine, get_fts_columns
from apps.dw.patchlib.fts_builder import build_like_fts


LOGGER = logging.getLogger("dw.sql_builder")

try:  # pragma: no cover - optional settings backend
    from apps.dw.settings_util import get_setting as _get_setting
except Exception:  # pragma: no cover - fallback used in tests
    def _get_setting(key: str, *, scope=None, namespace=None, default=None):
        return default

from apps.dw.fts_utils import DEFAULT_CONTRACT_FTS_COLUMNS
from apps.dw.settings_defaults import DEFAULT_EXPLICIT_FILTER_COLUMNS


def _wrap_ci_trim(col_expr: str, bind_name: str, ci: bool, trim: bool) -> str:
    """Build a case-insensitive / trimmed equality predicate when requested."""

    col = col_expr
    val = f":{bind_name}"
    if trim:
        col = f"TRIM({col})"
        val = f"TRIM({val})"
    if ci:
        col = f"UPPER({col})"
        val = f"UPPER({val})"
    return f"{col} = {val}"


def build_eq_where(
    eq_filters: List[Dict[str, Any]],
    binds: Dict[str, Any],
    *,
    allowed_columns: Optional[Iterable[str]] = None,
) -> List[str]:
    """Translate equality filters into SQL predicates limited to allowed columns."""

    if not eq_filters:
        return []

    if allowed_columns is None:
        configured = _get_setting(
            "DW_EXPLICIT_FILTER_COLUMNS",
            scope="namespace",
            namespace="dw::common",
            default=None,
        )
        if configured is None:
            configured = DEFAULT_EXPLICIT_FILTER_COLUMNS
    else:
        configured = allowed_columns

    allowed_set = {
        str(col).strip().upper().replace(" ", "_")
        for col in configured
        if isinstance(col, str) and col.strip()
    }

    predicates: List[str] = []
    idx = 0
    for filt in eq_filters:
        col = str(filt.get("col") or "").strip().upper()
        if not col or col not in allowed_set:
            continue
        bind_name = f"eq_{idx}"
        binds[bind_name] = filt.get("val")
        predicates.append(
            _wrap_ci_trim(col, bind_name, bool(filt.get("ci")), bool(filt.get("trim")))
        )
        idx += 1
    return predicates


def build_fts_where_legacy(
    tokens: Sequence[str],
    binds: Dict[str, Any],
    operator: str = "OR",
    *,
    columns: Optional[Iterable[str]] = None,
) -> str:
    """Build a LIKE-based FTS predicate over configured columns."""

    token_list = [tok.strip() for tok in tokens or [] if isinstance(tok, str) and tok.strip()]
    if not token_list:
        return ""

    if columns is None:
        mapping = _get_setting(
            "DW_FTS_COLUMNS",
            scope="namespace",
            namespace="dw::common",
            default={},
        )
        if isinstance(mapping, dict):
            column_candidates = mapping.get("Contract") or mapping.get("CONTRACT") or mapping.get("*")
        else:
            column_candidates = None
        if not column_candidates:
            column_candidates = DEFAULT_CONTRACT_FTS_COLUMNS
    else:
        column_candidates = columns

    cols = [
        col if (isinstance(col, str) and col.strip().startswith("\""))
        else str(col).strip().upper()
        for col in column_candidates
        if isinstance(col, str) and col.strip()
    ]

    if not cols:
        return ""

    groups: List[str] = []
    for idx, token in enumerate(token_list):
        bind_name = f"fts_{idx}"
        binds[bind_name] = f"%{token}%"
        per_column = [f"UPPER(NVL({col},'')) LIKE UPPER(:{bind_name})" for col in cols]
        groups.append("(" + " OR ".join(per_column) + ")")

    joiner = " AND " if (operator or "OR").strip().upper() == "AND" else " OR "
    return "(" + joiner.join(groups) + ")"


build_fts_where_with_binds = build_fts_where_legacy


def build_measure_sql() -> str:
    return (
        "NVL(CONTRACT_VALUE_NET_OF_VAT,0) + CASE WHEN NVL(VAT,0) BETWEEN 0 AND 1 "
        "THEN NVL(CONTRACT_VALUE_NET_OF_VAT,0) * NVL(VAT,0) ELSE NVL(VAT,0) END"
    )


def build_eq_where_from_pairs(eq_pairs: List[Dict], synonyms: Dict) -> Tuple[str, Dict[str, Any]]:
    """
    Build CASE-insensitive, TRIM-aware equality WHERE.
    Special-case REQUEST_TYPE with DW_ENUM_SYNONYMS.
    """

    clauses: List[str] = []
    binds: Dict[str, Any] = {}
    bidx = 0
    for e in eq_pairs or []:
        col = str(e.get("col", "")).upper()
        val = e.get("val")
        if not col:
            continue
        if col == "REQUEST_TYPE":
            syn = (synonyms or {}).get("Contract.REQUEST_TYPE") or {}
            or_parts: List[str] = []

            equals_vals: List[str] = []
            for cat in syn.values():
                equals_vals += [v for v in cat.get("equals", []) if v]
            if equals_vals:
                eq_bind_names: List[str] = []
                for v in list(dict.fromkeys(equals_vals)):
                    bn = f"eq_{bidx}"; bidx += 1
                    binds[bn] = v
                    eq_bind_names.append(bn)
                or_parts.append(
                    "(UPPER(TRIM(REQUEST_TYPE)) IN ("
                    + ", ".join([f"UPPER(TRIM(:{bn}))" for bn in eq_bind_names])
                    + "))"
                )

            prefixes: List[str] = []
            for cat in syn.values():
                prefixes += [v for v in cat.get("prefix", []) if v]
            for p in list(dict.fromkeys(prefixes)):
                bn = f"eq_{bidx}"; bidx += 1
                binds[bn] = f"{p}%"
                or_parts.append("UPPER(TRIM(REQUEST_TYPE)) LIKE UPPER(TRIM(:{}))".format(bn))

            contains: List[str] = []
            for cat in syn.values():
                contains += [v for v in cat.get("contains", []) if v]
            for c in list(dict.fromkeys(contains)):
                bn = f"eq_{bidx}"; bidx += 1
                binds[bn] = f"%{c}%"
                or_parts.append("UPPER(TRIM(REQUEST_TYPE)) LIKE UPPER(TRIM(:{}))".format(bn))

            if or_parts:
                clauses.append("(" + " OR ".join(or_parts) + ")")
            else:
                bn = f"eq_{bidx}"; bidx += 1
                binds[bn] = val
                clauses.append("UPPER(TRIM(REQUEST_TYPE)) = UPPER(TRIM(:{}))".format(bn))
        else:
            bn = f"eq_{bidx}"; bidx += 1
            binds[bn] = val
            clauses.append(f"UPPER(TRIM({col})) = UPPER(TRIM(:{bn}))")

    where_sql = " AND ".join(clauses) if clauses else ""
    return where_sql, binds


def build_fts_where(tokens: Sequence[str], mode: str = "OR") -> Tuple[str, Dict[str, Any]]:
    eng = get_fts_engine()
    columns = [c for c in get_fts_columns("Contract") if c]
    if not columns:
        return "", {}

    toks = [t for t in (tokens or []) if t]
    if not toks:
        return "", {}

    if (mode or "OR").upper() == "AND":
        groups = [[t] for t in toks]
    else:
        groups = [toks]

    if eng == "like":
        return build_like_fts(columns, groups)
    return build_like_fts(columns, groups)


GROSS_EXPR = (
    "NVL(CONTRACT_VALUE_NET_OF_VAT,0) + CASE WHEN NVL(VAT,0) BETWEEN 0 AND 1 "
    "THEN NVL(CONTRACT_VALUE_NET_OF_VAT,0) * NVL(VAT,0) ELSE NVL(VAT,0) END"
)


def _merge_where(parts):
    parts = [part for part in parts if part]
    if not parts:
        return ""
    return "WHERE " + " AND ".join(f"({part})" for part in parts)


def build_contract_sql(intent: Dict[str, Any]) -> Tuple[str, Dict[str, str]]:
    """Lightweight SQL builder used by the simplified DW blueprint."""

    selects = "*"
    table = '"Contract"'
    binds: Dict[str, str] = {}
    where_parts: List[str] = []

    fts_info = intent.get("fts") or {}
    if fts_info.get("enabled") and fts_info.get("where"):
        where_parts.append(str(fts_info.get("where")))
        binds.update({k: v for k, v in (fts_info.get("binds") or {}).items()})

    eq_info = intent.get("eq") or {}
    if eq_info.get("where"):
        where_parts.append(str(eq_info.get("where")))
        binds.update({k: v for k, v in (eq_info.get("binds") or {}).items()})

    group_by = intent.get("group_by") or []
    gross = intent.get("gross")

    order_col = (intent.get("sort_by") or "REQUEST_DATE").upper()
    order_dir = "DESC" if intent.get("sort_desc", True) else "ASC"

    where_sql = _merge_where(where_parts)
    if where_sql:
        where_sql = where_sql + "\n"

    if group_by:
        gb_list = [str(col).upper().replace(" ", "_") for col in group_by if str(col).strip()]
        gb_sql = ", ".join(gb_list)
        if gross:
            selects = f"{gb_sql} AS GROUP_KEY, SUM({GROSS_EXPR}) AS MEASURE, COUNT(*) AS CNT"
            order_col = "MEASURE"
        else:
            selects = f"{gb_sql} AS GROUP_KEY, COUNT(*) AS CNT"
            order_col = "CNT"
        sql = (
            f"SELECT {selects}\n"
            f"FROM {table}\n"
            f"{where_sql}"
            f"GROUP BY {gb_sql}\n"
            f"ORDER BY {order_col} {order_dir}"
        )
        return sql, binds

    sql = (
        f"SELECT {selects}\n"
        f"FROM {table}\n"
        f"{where_sql}"
        f"ORDER BY {order_col} {order_dir}"
    )
    return sql, binds


def quote_ident(name: str) -> str:
    if not name:
        return name
    n = name.strip()
    if n.startswith('"') and n.endswith('"'):
        return n
    return '"' + n.upper() + '"'


def strip_double_order_by(sql: str) -> str:
    parts = sql.split("\nORDER BY ")
    if len(parts) <= 2:
        return sql
    return parts[0] + "\nORDER BY " + parts[1]


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


def _build_fts_where_from_intent(intent: Dict[str, Any], bind_prefix: str = "fts") -> Tuple[Optional[str], Dict[str, Any]]:
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


def build_fts_where_from_intent(intent: Dict[str, Any], bind_prefix: str = "fts") -> Tuple[Optional[str], Dict[str, Any]]:
    """Compatibility wrapper for callers expecting the legacy API name."""

    return _build_fts_where_from_intent(intent, bind_prefix=bind_prefix)


# Backwards compatibility alias
build_fts_where_legacy = build_fts_where_from_intent


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

    fts_clause, fts_binds = _build_fts_where_from_intent(it)
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
