from __future__ import annotations

from typing import Any, Dict, List, Tuple

try:  # pragma: no cover - shared settings helper
    from apps.settings import get_setting  # type: ignore
except Exception:  # pragma: no cover - fallback to DW legacy settings accessor
    try:
        from apps.dw.settings import get_setting  # type: ignore
    except Exception:
        def get_setting(*_args: Any, **_kwargs: Any) -> Any:  # type: ignore[return-type]
            return _kwargs.get("default")

try:  # pragma: no cover - optional helper
    from apps.dw.utils import to_upper_trim as _external_to_upper_trim
except Exception:  # pragma: no cover - fallback when helper absent
    _external_to_upper_trim = None  # type: ignore[assignment]

from apps.dw.logger import log
from apps.dw.shared_sql_exec import run_select_with_columns
from apps.dw.store import load_inquiry

logger = log

try:  # pragma: no cover - allow import when optional dependencies missing
    from apps.dw.settings import (
        get_namespace_settings,
        get_setting_json,
        get_setting_value,
    )
except Exception:  # pragma: no cover - fallback for tests without full stack
    def get_namespace_settings(*_args, **_kwargs):  # type: ignore[return-type]
        return {}

    def get_setting_json(*_args, **_kwargs):  # type: ignore[return-type]
        return {}

    def get_setting_value(*_args, **_kwargs):  # type: ignore[return-type]
        return None


def _to_upper_trim(col_sql: str) -> str:
    if callable(_external_to_upper_trim):
        try:
            return _external_to_upper_trim(col_sql)
        except Exception:
            pass
    return f"UPPER(TRIM({col_sql}))"


def _wrap_value_bind(bind_name: str) -> str:
    return f"UPPER(:{bind_name})"


def _execute_and_sample(
    sql: str,
    binds: Dict[str, Any],
    *,
    sample_rows: int | None = None,
    datasource: str | None = None,
):
    """
    Execute SELECT and return (columns, rows, rowcount).
    This is RATE-only helper. Does FETCH; does NOT affect /dw/answer.
    """

    try:
        default_rows = get_setting("DW_RATE_SAMPLE_ROWS", default=25)
    except Exception:
        default_rows = 25

    if sample_rows is None:
        try:
            sample_rows = int(default_rows)
        except (TypeError, ValueError):
            sample_rows = 25

    sample_rows = max(sample_rows or 0, 0)
    effective_binds: Dict[str, Any] = dict(binds or {})

    columns, all_rows = run_select_with_columns(sql, effective_binds, datasource=datasource)
    if sample_rows and sample_rows > 0:
        rows = all_rows[:sample_rows]
    else:
        rows = all_rows
    return columns, rows, len(all_rows)


def _normalize_fts_columns(raw: Any) -> List[str]:
    seen: set[str] = set()
    result: List[str] = []
    if not isinstance(raw, list):
        return result
    for col in raw:
        text = str(col or "").strip()
        if not text:
            continue
        cleaned = text.strip('"')
        key = cleaned.upper()
        if key in seen:
            continue
        seen.add(key)
        result.append(cleaned)
    return result


def _load_context(inquiry_id: Any | None, *, override_table: str | None = None) -> Dict[str, Any]:
    settings_map = get_namespace_settings("dw::common") or {}
    table_source = override_table if override_table is not None else settings_map.get("DW_CONTRACT_TABLE")
    table_raw = table_source or "Contract"
    table_clean = str(table_raw or "").strip()
    if table_clean.startswith('"') and table_clean.endswith('"'):
        table_clean = table_clean.strip('"')
    table = table_clean or "Contract"

    datasource = settings_map.get("DEFAULT_DATASOURCE") or "docuware"
    fts_engine = str(settings_map.get("DW_FTS_ENGINE") or "like").strip().lower() or "like"
    fts_map = settings_map.get("DW_FTS_COLUMNS")
    if not isinstance(fts_map, dict):
        fts_map = get_setting_json("DW_FTS_COLUMNS", scope="namespace") or {}

    candidates_keys = [
        override_table,
        table_raw,
        table,
        table.upper(),
        table.lower(),
        table_clean,
        "Contract",
        "CONTRACT",
        "*",
    ]
    columns_raw: List[str] = []
    if isinstance(fts_map, dict):
        for key in candidates_keys:
            if isinstance(key, str) and key in fts_map and isinstance(fts_map[key], list):
                columns_raw = list(fts_map[key])
                if columns_raw:
                    break
    fts_columns = _normalize_fts_columns(columns_raw) if columns_raw else []
    if not fts_columns and isinstance(fts_map, dict):
        wildcard = fts_map.get("*")
        if isinstance(wildcard, list):
            fts_columns = _normalize_fts_columns(wildcard)

    date_col = str(settings_map.get("DW_DATE_COLUMN") or "REQUEST_DATE").strip() or "REQUEST_DATE"
    select_all = bool(settings_map.get("DW_SELECT_ALL_DEFAULT", True))
    empty_retry = bool(settings_map.get("EMPTY_RESULT_AUTORETRY", False))

    inquiry: Dict[str, Any] = {}
    try:
        inquiry_id_int = int(inquiry_id) if inquiry_id is not None else None
    except Exception:
        inquiry_id_int = None
    if inquiry_id_int is not None:
        loaded = load_inquiry(inquiry_id_int)
        if isinstance(loaded, dict):
            inquiry = dict(loaded)

    prefixes = inquiry.get("prefixes") if isinstance(inquiry.get("prefixes"), list) else []
    full_text_flag = bool(inquiry.get("full_text_search"))

    return {
        "table": table,
        "ds": datasource,
        "auth_email": inquiry.get("auth_email"),
        "fts_engine": fts_engine or "like",
        "fts_columns": fts_columns,
        "date_col": date_col,
        "select_all": select_all,
        "empty_retry": empty_retry,
        "full_text_search": full_text_flag,
        "prefixes": prefixes,
    }


def _expand_eq_alias(table: str, col: str) -> List[str]:
    aliases = get_setting_json("DW_EQ_ALIAS_COLUMNS", scope="namespace")
    if not isinstance(aliases, dict):
        aliases = {}
    targets = aliases.get(col.upper(), [])
    if not targets:
        return [col]
    return targets


def _synonyms_for_enum(table_dot_col: str, raw_value: str) -> Dict[str, List[str]]:
    syn = get_setting_json("DW_ENUM_SYNONYMS", scope="namespace") or {}
    mapping = syn.get(table_dot_col, {}) if isinstance(syn, dict) else {}
    raw_norm = raw_value.strip().lower()
    for rule in mapping.values():
        if not isinstance(rule, dict):
            continue
        equals = [s for s in rule.get("equals", []) if isinstance(s, str)]
        prefix = [s for s in rule.get("prefix", []) if isinstance(s, str)]
        contains = [s for s in rule.get("contains", []) if isinstance(s, str)]
        if raw_norm in [e.strip().lower() for e in equals]:
            return {"equals": equals, "prefix": prefix, "contains": contains}
    return {"equals": [raw_value], "prefix": [], "contains": []}


def build_eq_condition(
    table: str,
    col: str,
    values: List[str],
    binds: Dict[str, str],
) -> Tuple[str, Dict[str, str]]:
    target_cols = _expand_eq_alias(table, col)
    wheres_or: List[str] = []

    for val in values:
        if col.upper() == "REQUEST_TYPE":
            syn = _synonyms_for_enum(f"{table}.{col}", val)
            eq_binds: List[str] = []
            pre_binds: List[str] = []
            con_binds: List[str] = []

            for s in syn.get("equals", []):
                bname = f"eq_{len(binds)}"
                binds[bname] = s.upper()
                eq_binds.append(_wrap_value_bind(bname))
            for s in syn.get("prefix", []):
                bname = f"eq_{len(binds)}"
                binds[bname] = f"{s.upper()}%"
                pre_binds.append(_wrap_value_bind(bname))
            for s in syn.get("contains", []):
                bname = f"eq_{len(binds)}"
                binds[bname] = f"%{s.upper()}%"
                con_binds.append(_wrap_value_bind(bname))

            per_value_targets: List[str] = []
            for tcol in target_cols:
                col_sql = _to_upper_trim(tcol)
                parts: List[str] = []
                if eq_binds:
                    parts.append(f"{col_sql} IN ({', '.join(eq_binds)})")
                if pre_binds:
                    parts.append(" OR ".join([f"{col_sql} LIKE {b}" for b in pre_binds]))
                if con_binds:
                    parts.append(" OR ".join([f"{col_sql} LIKE {b}" for b in con_binds]))
                if parts:
                    per_value_targets.append("(" + " OR ".join(parts) + ")")
            if per_value_targets:
                wheres_or.append("(" + " OR ".join(per_value_targets) + ")")
        else:
            bname = f"eq_{len(binds)}"
            binds[bname] = val.upper()
            per_value_targets = []
            for tcol in target_cols:
                col_sql = _to_upper_trim(tcol)
                per_value_targets.append(f"{col_sql} = {_wrap_value_bind(bname)}")
            wheres_or.append("(" + " OR ".join(per_value_targets) + ")")

    if not wheres_or:
        return "1=1", binds
    return "(" + " OR ".join(wheres_or) + ")", binds


def build_fts_or_clause(columns: List[str], bind_name: str) -> str:
    terms: List[str] = []
    for col in columns:
        text = str(col or "").strip()
        if not text:
            continue
        cleaned = text.strip('"')
        col_sql = f'"{cleaned}"'
        terms.append(f"UPPER({col_sql}) LIKE UPPER(:{bind_name})")
    if not terms:
        return "1=1"
    return "(" + " OR ".join(terms) + ")"


def build_fts_like(
    columns: List[str], tokens_groups: List[List[str]], binds: Dict[str, str]
) -> Tuple[str, Dict[str, str]]:
    if not columns or not tokens_groups:
        return "1=1", binds

    next_index = 0
    for key in binds:
        if key.startswith("fts_"):
            try:
                idx = int(key.split("_", 1)[1])
            except (ValueError, IndexError):
                continue
            next_index = max(next_index, idx + 1)

    result_binds = dict(binds)
    clauses: List[str] = []
    for group in tokens_groups:
        cleaned_tokens = [str(token).strip() for token in (group or []) if str(token or "").strip()]
        if not cleaned_tokens:
            continue
        per_token_clauses: List[str] = []
        for token in cleaned_tokens:
            bind_name = f"fts_{next_index}"
            next_index += 1
            result_binds[bind_name] = f"%{token}%"
            per_token_clauses.append(build_fts_or_clause(columns, bind_name))
        if not per_token_clauses:
            continue
        clause = " AND ".join(per_token_clauses)
        if len(per_token_clauses) > 1:
            clause = "(" + clause + ")"
        clauses.append(clause)

    if not clauses:
        return "1=1", result_binds
    return "(" + " OR ".join(clauses) + ")", result_binds


def build_select_all(table: str, order_by: str | None = None, desc: bool = True) -> str:
    """
    Build a simple SELECT * with optional ORDER BY.
    NOTE: `order_by` must be validated/whitelisted by caller.
    """
    base = f'SELECT * FROM "{table}"'
    if order_by:
        direction = "DESC" if desc else "ASC"
        # Use a triple-quoted f-string for multi-line SQL
        return f"""SELECT * FROM "{table}"
ORDER BY {order_by} {direction}"""
    return base


def assemble_sql(
    table: str,
    where_parts: List[str],
    order_by: str | None,
    *,
    default_order: str | None = None,
) -> str:
    where_sql = " AND ".join([part for part in where_parts if part and part.strip()])
    if not where_sql:
        where_sql = "1=1"
    if not order_by:
        fallback = default_order or get_setting_value("DW_DATE_COLUMN", scope="namespace")
        fallback = str(fallback or "REQUEST_DATE").strip() or "REQUEST_DATE"
        order_by = f"{fallback} DESC"
    return f"{build_select_all(table)}\nWHERE {where_sql}\nORDER BY {order_by}"


def run_rate(
    comment: str,
    table: str = "Contract",
    *,
    inquiry_id: Any | None = None,
    sample_rows: int | None = None,
) -> Dict[str, object]:
    ctx = _load_context(inquiry_id, override_table=table)
    target_table = ctx.get("table") or table or "Contract"
    ctx["table"] = target_table

    pieces = [piece.strip() for piece in comment.split(";") if piece.strip()]
    eq_map: Dict[str, List[str]] = {}
    fts_groups: List[List[str]] = []
    order_by: str | None = None

    for piece in pieces:
        lower = piece.lower()
        if lower.startswith("eq:"):
            rhs = piece[3:].strip()
            if "=" in rhs:
                col, vals = rhs.split("=", 1)
                col = col.strip()
                raw_vals = [v.strip() for v in vals.split(" or ") if v.strip()]
                if raw_vals:
                    eq_map[col] = raw_vals
        elif lower.startswith("fts:"):
            rhs = piece[4:].strip()
            parts = [token.strip() for token in rhs.split(" or ") if token.strip()]
            fts_groups = [[token] for token in parts]
        elif lower.startswith("order_by:"):
            order_by = piece.split(":", 1)[1].strip()

    binds: Dict[str, str] = {}
    wheres: List[str] = []

    for col, vals in eq_map.items():
        where_eq, binds = build_eq_condition(target_table, col, vals, binds)
        wheres.append(where_eq)

    if fts_groups:
        where_fts, binds = build_fts_like(ctx.get("fts_columns", []), fts_groups, binds)
        wheres.append(where_fts)

    final_sql = assemble_sql(
        target_table,
        wheres,
        order_by,
        default_order=ctx.get("date_col"),
    )
    binds_copy = dict(binds)

    intent_debug = {
        "eq_filters": [{"col": col, "values": vals} for col, vals in eq_map.items()],
        "fts_groups": fts_groups,
        "sort_by": order_by or f"{ctx.get('date_col', 'REQUEST_DATE')} DESC",
    }
    validation_debug = {
        "ok": True,
        "bind_names": list(binds_copy.keys()),
        "binds": binds_copy,
        "errors": [],
    }

    if hasattr(logger, "debug"):
        logger.debug("rate_pipeline.sql", extra={"sql": final_sql, "binds": binds_copy})

    logger.info(
        {
            "event": "rate.sql.exec",
            "inquiry_id": inquiry_id,
            "sql": final_sql,
            "binds": binds_copy,
            "datasource": ctx.get("ds"),
            "auth_email": ctx.get("auth_email"),
        }
    )

    columns, rows, rc = _execute_and_sample(
        final_sql,
        binds_copy,
        sample_rows=sample_rows,
        datasource=ctx.get("ds"),
    )

    logger.info({"event": "rate.sql.done", "inquiry_id": inquiry_id, "rows": rc})

    resp = {
        "ok": True,
        "retry": False,
        "inquiry_id": inquiry_id,
        "sql": final_sql,
        "binds": binds_copy,
        "columns": columns,
        "rows": rows,
        "debug": {
            "final_sql": {"sql": final_sql, "size": len(final_sql)},
            "intent": intent_debug,
            "validation": validation_debug,
            "context": {
                "datasource": ctx.get("ds"),
                "table": target_table,
                "auth_email": ctx.get("auth_email"),
                "fts_engine": ctx.get("fts_engine"),
                "fts_columns": ctx.get("fts_columns"),
                "full_text_search": ctx.get("full_text_search"),
                "prefixes": ctx.get("prefixes"),
            },
        },
    }

    return resp


__all__ = [
    "assemble_sql",
    "build_eq_condition",
    "build_select_all",
    "build_fts_or_clause",
    "build_fts_like",
    "run_rate",
]
