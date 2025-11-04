from __future__ import annotations

import re
from typing import Any, Dict, List, Tuple
from collections import defaultdict

from apps.dw.filters import build_boolean_groups_where
from apps.dw.fts import build_fts_clause
from apps.dw.common.eq_aliases import resolve_eq_targets

from .intent import NLIntent
from .sql_builders import window_predicate
from .utils import env_flag
from apps.dw.lib.sql_utils import in_expr, upper_trim, or_join

TABLE = '"Contract"'


def _cfg_get(cfg: Any, key: str, default: Any = None) -> Any:
    if isinstance(cfg, dict):
        return cfg.get(key, default)
    getter = getattr(cfg, "get", None)
    if callable(getter):
        try:
            value = getter(key, default)
        except TypeError:
            value = getter(key)
        if value is not None:
            return value
    return default


def assemble_query(intent: dict, cfg: Any) -> dict:
    table = _cfg_get(cfg, "DW_CONTRACT_TABLE", "Contract")
    sql = f'SELECT * FROM "{table}"'
    binds: Dict[str, Any] = {}
    where_parts: List[str] = []

    fts_sql, fts_binds, _ = build_fts_clause(
        table,
        intent.get("fts_groups", []),
        intent.get("fts_operator", "OR"),
        cfg,
    )
    if fts_sql:
        where_parts.append(fts_sql)
        binds.update(fts_binds)

    bg_where, bg_binds = build_boolean_groups_where(intent.get("boolean_groups") or [], cfg)
    if bg_where:
        where_parts.append(bg_where)
        binds.update(bg_binds)

    if where_parts:
        sql += "\nWHERE " + " AND ".join(where_parts)

    date_column = _cfg_get(cfg, "DW_DATE_COLUMN", "REQUEST_DATE")
    sort_by = intent.get("sort_by") or date_column
    sort_desc = intent.get("sort_desc")
    if sort_desc is None:
        sort_desc = True
    sort_by = str(sort_by or "").strip()
    if not sort_by:
        sort_by = str(date_column or "REQUEST_DATE").strip()
    sort_by = sort_by.replace("_DESC", "")
    direction = " DESC" if sort_desc else ""
    sql += f"\nORDER BY {sort_by}{direction}"

    return {"sql": sql, "binds": binds}


def _gross_expr() -> str:
    return (
        "NVL(CONTRACT_VALUE_NET_OF_VAT,0) + "
        "CASE WHEN NVL(VAT,0) BETWEEN 0 AND 1 "
        "THEN NVL(CONTRACT_VALUE_NET_OF_VAT,0) * NVL(VAT,0) "
        "ELSE NVL(VAT,0) END"
    )


def _where_from_eq_filters(eq_filters: List[dict], binds: Dict[str, Any]) -> str:
    clauses: List[str] = []
    for idx, raw in enumerate(eq_filters or []):
        col = (raw.get("col") or raw.get("column") or "").strip()
        if not col:
            continue
        op = (raw.get("op") or ("like" if "pattern" in raw else "eq")).lower()
        val = (
            raw.get("val")
            if raw.get("val") is not None
            else raw.get("value")
            if raw.get("value") is not None
            else raw.get("pattern")
        )
        synonyms = raw.get("synonyms") if isinstance(raw.get("synonyms"), dict) else None
        ci = bool(raw.get("ci"))
        trim = bool(raw.get("trim"))

        def _compose(bind_name: str, operator: str) -> str:
            col_expr = col.upper()
            rhs_expr = f":{bind_name}"
            if trim:
                col_expr = f"TRIM({col_expr})"
                rhs_expr = f"TRIM({rhs_expr})"
            if ci:
                col_expr = f"UPPER({col_expr})"
                rhs_expr = f"UPPER({rhs_expr})"
            if operator == "like":
                return f"{col_expr} LIKE {rhs_expr}"
            op_map = {
                "eq": "=",
                "gt": ">",
                "gte": ">=",
                "lt": "<",
                "lte": "<=",
            }
            sql_op = op_map.get(operator, "=")
            return f"{col_expr} {sql_op} {rhs_expr}"

        if synonyms:
            equals_vals = [v for v in synonyms.get("equals", []) if v]
            prefix_vals = [v for v in synonyms.get("prefix", []) if v]
            contains_vals = [v for v in synonyms.get("contains", []) if v]
            terms: List[str] = []
            for j, eq_val in enumerate(equals_vals):
                bind_name = f"eq_{idx}" if j == 0 else f"eq_{idx}_{j}"
                bind_value = eq_val.strip() if trim and isinstance(eq_val, str) else eq_val
                binds[bind_name] = bind_value
                terms.append(_compose(bind_name, "eq"))
            for j, pre_val in enumerate(prefix_vals):
                bind_name = f"pre_{idx}_{j}"
                binds[bind_name] = f"{pre_val}%"
                terms.append(_compose(bind_name, "like"))
            for j, contains_val in enumerate(contains_vals):
                bind_name = f"con_{idx}_{j}"
                binds[bind_name] = f"%{contains_val}%"
                terms.append(_compose(bind_name, "like"))
            if terms:
                clauses.append("(" + " OR ".join(terms) + ")")
                continue

        if val is None:
            continue

        bind = f"eq_{idx}"
        bind_val = val
        if trim and isinstance(bind_val, str):
            bind_val = bind_val.strip()
        if op == "like" and isinstance(bind_val, str) and "%" not in bind_val:
            bind_val = f"%{bind_val}%"

        binds[bind] = bind_val
        clauses.append(_compose(bind, op))

    return " AND ".join(clauses)


# --- New helpers for IN-based EQ grouping and OR group building ---

def _normalize_eq_entry(item: Any) -> Tuple[str, List[Any]]:
    if isinstance(item, dict):
        col = item.get("col") or item.get("column") or item.get("field")
        values = item.get("values")
        if values is None:
            candidate = item.get("val") if item.get("val") is not None else item.get("value")
            if isinstance(candidate, (list, tuple, set)):
                values = list(candidate)
            elif candidate is not None:
                values = [candidate]
        return str(col or ""), list(values or [])
    if isinstance(item, (list, tuple)) and len(item) >= 2:
        col = item[0]
        vals = item[1]
        if isinstance(vals, (list, tuple, set)):
            values = list(vals)
        elif vals is None:
            values = []
        else:
            values = [vals]
        return str(col or ""), values
    return "", []


def _eq_clause_from_filters(
    eq_filters,
    binds: Dict[str, Any],
    *,
    bind_prefix: str = "eq",
) -> Tuple[str, Dict[str, Any], Dict[str, List[str]]]:
    """Build equality clause using IN(...) per alias, and report expanded targets."""
    from apps.dw.lib import sql_utils

    alias_values: Dict[str, List[Any]] = defaultdict(list)
    for item in eq_filters or []:
        col_raw, values = _normalize_eq_entry(item)
        if not col_raw:
            continue
        col_key = str(col_raw).strip().upper()
        if not col_key:
            continue
        for value in values or []:
            alias_values[col_key].append(value)

    if not alias_values:
        return "", binds, {}

    used_names: set[str] = {str(k) for k in binds.keys() if isinstance(k, str)}
    temp_binds: Dict[str, Any] = dict(binds)
    clauses: List[str] = []
    alias_targets_map: Dict[str, List[str]] = {}

    def _alloc(counter: List[int]) -> str:
        while True:
            name = f"{bind_prefix}_{counter[0]}"
            counter[0] += 1
            if name not in used_names:
                used_names.add(name)
                return name

    for alias, raw_values in alias_values.items():
        targets = resolve_eq_targets(alias) or [alias]
        targets = [str(t or "").strip().upper() for t in targets if str(t or "").strip()]
        if not targets:
            continue
        alias_targets_map[alias] = targets

        dedup_values: List[Any] = []
        seen_keys: set[Any] = set()
        for value in raw_values:
            if value is None:
                continue
            candidate = value
            key: Any
            if isinstance(candidate, str):
                stripped = candidate.strip()
                if not stripped:
                    continue
                key = stripped.upper()
                candidate = stripped.upper()
            else:
                key = candidate
            if key in seen_keys:
                continue
            seen_keys.add(key)
            dedup_values.append(candidate)
        if not dedup_values:
            continue

        bind_names: List[str] = []
        counter = [0]
        for candidate in dedup_values:
            bind_name = _alloc(counter)
            temp_binds[bind_name] = candidate
            bind_names.append(bind_name)

        per_column = [
            sql_utils.in_expr(col, bind_names)
            for col in targets
            if col and bind_names
        ]
        per_column = [expr for expr in per_column if expr]
        if not per_column:
            continue
        if len(per_column) == 1:
            clauses.append(per_column[0])
        else:
            clauses.append(sql_utils.or_join(per_column))

    combined = " AND ".join(expr for expr in clauses if expr)
    return combined, temp_binds, alias_targets_map


def _compose_in_clause(column: str, bind_names: List[str], *, ci: bool = True, trim: bool = True) -> str:
    if not bind_names:
        return ""
    col_sql = column.strip().upper()
    if not col_sql:
        return ""
    lhs = col_sql
    if trim:
        lhs = f"TRIM({lhs})"
    if ci:
        lhs = f"UPPER({lhs})"
    rhs_terms: List[str] = []
    for name in bind_names:
        rhs = f":{name}"
        if ci:
            rhs = f"UPPER({rhs})"
        rhs_terms.append(rhs)
    return f"{lhs} IN ({', '.join(rhs_terms)})"


def build_or_group(or_terms: List[dict]) -> Tuple[str, Dict[str, Any]]:
    """
    Build a single OR group from equality-like terms, reusing bind names across columns.
    Returns a tuple of the SQL snippet and the bind dictionary.
    """
    from apps.dw.lib import sql_utils

    if not or_terms:
        return "", {}

    binds: Dict[str, Any] = {}
    value_to_name: Dict[Any, str] = {}
    column_order: List[str] = []
    column_bind_map: Dict[str, List[str]] = {}
    column_flags: Dict[str, Dict[str, bool]] = {}

    def _coerce_flag(value: Any):
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        if isinstance(value, str):
            lowered = value.strip().lower()
            if lowered in {"1", "true", "t", "yes", "y", "on"}:
                return True
            if lowered in {"0", "false", "f", "no", "n", "off"}:
                return False
        return None

    def _ensure_bind(value: Any) -> str:
        key = value
        if isinstance(value, str):
            key = value.strip().upper()
        if key not in value_to_name:
            name = f"eq_{len(value_to_name)}"
            stored = value.strip() if isinstance(value, str) else value
            value_to_name[key] = name
            binds[name] = stored
        return value_to_name[key]

    for item in or_terms or []:
        col = ""
        values: List[Any] = []
        ci_flag = None
        trim_flag = None

        if isinstance(item, dict):
            col = str(item.get("col") or item.get("column") or item.get("field") or "").strip().upper()
            raw_vals = (
                item.get("values")
                if item.get("values") is not None
                else item.get("val")
                if item.get("val") is not None
                else item.get("value")
            )
            if isinstance(raw_vals, (list, tuple, set)):
                values = list(raw_vals)
            elif raw_vals is not None:
                values = [raw_vals]
            ci_flag = _coerce_flag(item.get("ci"))
            trim_flag = _coerce_flag(item.get("trim"))
        elif isinstance(item, (list, tuple)) and item:
            col = str(item[0] or "").strip().upper()
            raw_vals = item[1] if len(item) > 1 else None
            if isinstance(raw_vals, (list, tuple, set)):
                values = list(raw_vals)
            elif raw_vals is not None:
                values = [raw_vals]
            ci_flag = True
            trim_flag = True
        else:
            continue

        if not col or not values:
            continue

        if col not in column_order:
            column_order.append(col)
        bucket = column_bind_map.setdefault(col, [])
        flags = column_flags.setdefault(col, {"ci": True, "trim": True})
        if ci_flag is not None:
            flags["ci"] = flags["ci"] and bool(ci_flag)
        if trim_flag is not None:
            flags["trim"] = flags["trim"] and bool(trim_flag)

        seen_local = set()
        for v in values:
            if v is None:
                continue
            if isinstance(v, str):
                candidate = v.strip()
                if not candidate:
                    continue
                key = candidate.upper()
                value = candidate
            else:
                key = v
                value = v
            if key in seen_local:
                continue
            seen_local.add(key)
            bind_name = _ensure_bind(value)
            if bind_name not in bucket:
                bucket.append(bind_name)

    parts: List[str] = []
    for col in column_order:
        bind_names = column_bind_map.get(col, [])
        if not bind_names:
            continue
        flags = column_flags.get(col, {"ci": True, "trim": True})
        clause = _compose_in_clause(col, bind_names, ci=flags.get("ci", True), trim=flags.get("trim", True))
        if clause:
            parts.append(clause)

    return (sql_utils.or_join(parts) if parts else ""), binds


def _coerce_numeric_literal(value: Any) -> Any:
    if isinstance(value, (int, float)):
        return value
    if isinstance(value, str):
        text = value.strip().replace(",", "")
        if text == "":
            return value
        try:
            if "." in text:
                return float(text)
            return int(text)
        except ValueError:
            return value
    return value


def numeric_clause_from_filters(
    numeric_filters: List[Any],
    binds: Dict[str, Any],
    *,
    bind_prefix: str = "num",
) -> Tuple[str, Dict[str, Any]]:
    """Render numeric predicates without trimming/upper casing."""

    if not numeric_filters:
        return "", binds

    clauses: List[str] = []
    counter = 0
    seen: set[Tuple[str, str, Tuple[Any, ...]]] = set()

    def _slug(col_name: str) -> str:
        slug = re.sub(r"[^A-Z0-9]+", "_", col_name.upper())
        return slug.strip("_") or "COL"

    for raw in numeric_filters or []:
        col = ""
        op = ""
        values: List[Any] = []
        if isinstance(raw, dict):
            col = str(raw.get("col") or raw.get("column") or "").strip().upper()
            op = str(raw.get("op") or raw.get("operator") or "").strip().lower()
            raw_vals = raw.get("values")
            if raw_vals is None and raw.get("val") is not None:
                raw_vals = [raw.get("val")]
        else:
            raw_vals = None
            if isinstance(raw, (list, tuple)) and len(raw) >= 2:
                col = str(raw[0] or "").strip().upper()
                op = str(raw[1] or "").strip().lower()
                if len(raw) >= 3:
                    raw_vals = raw[2]

        if not col or not op:
            continue

        if isinstance(raw_vals, (list, tuple, set)):
            values = list(raw_vals)
        elif raw_vals is None:
            values = []
        else:
            values = [raw_vals]

        normalized_values = tuple(_coerce_numeric_literal(v) for v in values)
        key = (col, op, normalized_values)
        if key in seen:
            continue
        seen.add(key)

        slug = _slug(col)
        if op == "between":
            if len(normalized_values) != 2:
                continue
            lhs = f"{bind_prefix}_{slug}_{counter}_lo"
            rhs = f"{bind_prefix}_{slug}_{counter}_hi"
            counter += 1
            binds[lhs] = normalized_values[0]
            binds[rhs] = normalized_values[1]
            clauses.append(f"{col} BETWEEN :{lhs} AND :{rhs}")
            continue

        op_map = {
            "gt": ">",
            "gte": ">=",
            "lt": "<",
            "lte": "<=",
            "eq": "=",
            ">": ">",
            ">=": ">=",
            "<": "<",
            "<=": "<=",
        }
        sql_op = op_map.get(op)
        if sql_op is None:
            continue
        if not normalized_values:
            continue
        name = f"{bind_prefix}_{slug}_{counter}"
        counter += 1
        binds[name] = normalized_values[0]
        clauses.append(f"{col} {sql_op} :{name}")

    return (" AND ".join(clauses) if clauses else ""), binds


def apply_online_rate_hints(intent: dict, settings: Any) -> Tuple[str, Dict[str, Any]]:
    """
    Minimal application of online/rate EQ hints.
    Returns only the EQ WHERE + binds here; FTS/order handled elsewhere.
    """
    eq_filters = intent.get("eq_filters") or []
    clause, binds, _ = _eq_clause_from_filters(eq_filters, {}, bind_prefix="eq")
    return clause, binds


# Convenience public helpers (deterministic EQ/OR building)
def eq_clause_from_filters(eq_filters: List[Tuple[str, List[str]]], binds: Dict[str, Any], start_idx: int = 0) -> Tuple[str, int]:
    """Aggregate same-column values to IN(...). Returns (clause, next_bind_index)."""
    if not eq_filters:
        return "", start_idx
    per_col: Dict[str, List[str]] = {}
    for col, vals in eq_filters or []:
        if not col:
            continue
        per_col.setdefault(str(col).upper().strip(), []).extend(list(vals or []))
    clauses: List[str] = []
    next_idx = start_idx
    for col, vals in per_col.items():
        if not vals:
            continue
        names: List[str] = []
        for v in vals:
            name = f"eq_{next_idx}"
            next_idx += 1
            binds[name] = (v.upper() if isinstance(v, str) else v)
            names.append(name)
        col_sql = f'"{col}"'
        if len(names) == 1:
            clauses.append(f"{upper_trim(col_sql)} IN (UPPER(:{names[0]}))")
        else:
            clauses.append(in_expr(col_sql, names))
    return (" AND ".join(f"({c})" for c in clauses if c), next_idx)


def or_groups_clause(or_groups: List[List[Tuple[str, List[str]]]], binds: Dict[str, Any], start_idx: int = 0) -> Tuple[str, int]:
    """Build (colA IN (...) OR colB IN (...)) groups and return (clause, next_idx)."""
    idx = start_idx
    groups_sql: List[str] = []
    for grp in or_groups or []:
        parts: List[str] = []
        for col, vals in grp or []:
            if not vals:
                continue
            names: List[str] = []
            for v in vals:
                name = f"eq_{idx}"
                idx += 1
                binds[name] = (v.upper() if isinstance(v, str) else v)
                names.append(name)
            col_sql = f'"{str(col).upper().strip()}"'
            parts.append(in_expr(col_sql, names))
        if parts:
            groups_sql.append(or_join(parts))
    return (" AND ".join(f"({g})" for g in groups_sql if g), idx)


def build_sql(intent: NLIntent) -> Tuple[str, Dict[str, Any]]:
    binds: Dict[str, Any] = {}
    where_clauses = []
    order_clause = ""
    select_cols = "*"

    if intent.explicit_dates:
        binds["date_start"] = intent.explicit_dates["start"]
        binds["date_end"] = intent.explicit_dates["end"]
        if intent.expire:
            where_clauses.append("END_DATE BETWEEN :date_start AND :date_end")
        else:
            where_clauses.append(window_predicate(intent.date_column or "OVERLAP"))

    eq_clause = _where_from_eq_filters(getattr(intent, "eq_filters", []) or [], binds)
    if eq_clause:
        where_clauses.append(eq_clause)

    # Manual filters injected by planners (optional)
    manual_where = getattr(intent, "manual_where", None)
    if manual_where:
        where_clauses.append(f"({manual_where})")
    manual_binds = getattr(intent, "manual_binds", None)
    if isinstance(manual_binds, dict):
        binds.update(manual_binds)

    measure = intent.measure_sql or "NVL(CONTRACT_VALUE_NET_OF_VAT,0)"
    group_by = (intent.group_by or "").strip()
    sort_by = (intent.sort_by or "").strip()
    sort_desc = intent.sort_desc if intent.sort_desc is not None else True

    where_sql = " AND ".join(where_clauses)

    if intent.agg == "count" and not group_by:
        sql = f"SELECT COUNT(*) AS CNT FROM {TABLE}"
        if where_sql:
            sql += f"\nWHERE {where_sql}"
        return sql, binds

    if group_by:
        gb_cols = [c.strip() for c in group_by.split(",") if c.strip()]
        gb = ", ".join(gb_cols) if gb_cols else group_by
        wants_gross = bool(intent.gross) or sort_by.upper() == "TOTAL_GROSS"

        if wants_gross:
            gross = _gross_expr()
            sql = (
                f"SELECT {gb} AS GROUP_KEY,\n"
                f"       SUM({gross}) AS TOTAL_GROSS,\n"
                f"       COUNT(*) AS CNT\n"
                f"FROM {TABLE}"
            )
            if where_sql:
                sql += f"\nWHERE {where_sql}"
            sql += f"\nGROUP BY {gb}"
            sql += f"\nORDER BY TOTAL_GROSS {'DESC' if sort_desc else 'ASC'}"
            if intent.top_n:
                binds["top_n"] = intent.top_n
                sql += "\nFETCH FIRST :top_n ROWS ONLY"
            return sql, binds

        sql = (
            f"SELECT\n  {gb} AS GROUP_KEY,\n  SUM({measure}) AS MEASURE\nFROM {TABLE}"
        )
        if where_sql:
            sql += f"\nWHERE {where_sql}"
        sql += f"\nGROUP BY {gb}"
        sql += f"\nORDER BY MEASURE {'DESC' if sort_desc else 'ASC'}"
        if intent.top_n:
            binds["top_n"] = intent.top_n
            sql += "\nFETCH FIRST :top_n ROWS ONLY"
        return sql, binds

    wanted = (intent.notes or {}).get("projection")
    if wanted:
        select_cols = ", ".join(wanted)
        sql = f"SELECT {select_cols} FROM {TABLE}"
    elif env_flag("DW_SELECT_ALL_DEFAULT", True) or intent.wants_all_columns:
        sql = f"SELECT * FROM {TABLE}"
    else:
        sql = (
            "SELECT CONTRACT_ID, CONTRACT_OWNER, REQUEST_DATE, START_DATE, END_DATE, "
            "CONTRACT_VALUE_NET_OF_VAT, VAT FROM {table}".format(table=TABLE)
        )

    if where_sql:
        sql += f"\nWHERE {where_sql}"

    if sort_by:
        sql += f"\nORDER BY {sort_by} {'DESC' if sort_desc else 'ASC'}"
    elif getattr(intent, "user_requested_top_n", False):
        sql += f"\nORDER BY {measure} DESC"
    elif eq_clause:
        sql += "\nORDER BY REQUEST_DATE DESC"
    else:
        sql += f"\nORDER BY {measure} {'DESC' if sort_desc else 'ASC'}"

    if intent.user_requested_top_n and intent.top_n:
        binds["top_n"] = intent.top_n
        sql += "\nFETCH FIRST :top_n ROWS ONLY"

    return sql, binds
