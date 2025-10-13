from __future__ import annotations

from typing import Any, Dict, List, Tuple

try:  # pragma: no cover - optional dependency in lightweight tests
    from sqlalchemy import text
except Exception:  # pragma: no cover - fallback when SQLAlchemy missing
    text = lambda sql: sql  # type: ignore[assignment]

try:  # pragma: no cover - production helper, optional in tests
    from apps.common.db import get_engine  # type: ignore
except Exception:  # pragma: no cover - fallback for simplified stubs
    get_engine = None  # type: ignore[assignment]

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

try:
    from apps.dw.db import run_query
except ImportError:  # pragma: no cover - fallback for simplified stubs
    from apps.dw.db import fetch_rows as run_query
from apps.dw.logger import log

logger = log

try:  # pragma: no cover - allow import when optional dependencies missing
    from apps.dw.settings import get_setting_json, get_setting_value
except Exception:  # pragma: no cover - fallback for tests without full stack
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


def _like_bind(bind_name: str) -> str:
    return f"UPPER(:{bind_name})"


def _nvl_upper_like(col_sql: str, bind_name: str) -> str:
    return f"UPPER(NVL({col_sql},'')) LIKE {_like_bind(bind_name)}"


def _execute_and_sample(sql: str, binds: Dict[str, Any], sample_rows: int | None = None):
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

    if get_engine is None:
        rows = run_query(sql, effective_binds) if callable(run_query) else []  # type: ignore[arg-type]
        sliced_rows = rows[:sample_rows] if sample_rows and sample_rows > 0 else rows
        if sliced_rows and isinstance(sliced_rows[0], dict):
            columns = list(sliced_rows[0].keys())
            normalized_rows = [[row.get(col) for col in columns] for row in sliced_rows]
        else:
            columns = []
            normalized_rows = [list(row) if not isinstance(row, dict) else list(row.values()) for row in sliced_rows]
        return columns, normalized_rows, len(normalized_rows)

    eng = get_engine()
    with eng.connect() as conn:  # type: ignore[operator]
        res = conn.execute(text(sql), effective_binds)
        if sample_rows > 0:
            rows = res.fetchmany(sample_rows)
        else:
            rows = res.fetchall()

        cursor = getattr(res, "cursor", None)
        if cursor and getattr(cursor, "description", None):
            columns = [desc[0] for desc in cursor.description]
        else:
            try:
                columns = list(res.keys())
            except Exception:
                columns = []

        rows_list = [list(row) for row in rows]
        return columns, rows_list, len(rows_list)


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


def build_fts_like(
    table: str,
    tokens_groups: List[List[str]],
    binds: Dict[str, str],
) -> Tuple[str, Dict[str, str]]:
    cfg = get_setting_json("DW_FTS_COLUMNS", scope="namespace") or {}
    cols = cfg.get(table, cfg.get("*", [])) if isinstance(cfg, dict) else []
    if not cols:
        return "1=1", binds

    groups_or: List[str] = []
    for group in tokens_groups:
        per_group_or: List[str] = []
        for token in group:
            if not token:
                continue
            bname = f"fts_{len(binds)}"
            binds[bname] = f"%{token.upper()}%"
            per_token_or = [_nvl_upper_like(c, bname) for c in cols]
            per_group_or.append("(" + " OR ".join(per_token_or) + ")")
        if per_group_or:
            groups_or.append("(" + " OR ".join(per_group_or) + ")")

    if not groups_or:
        return "1=1", binds
    return "(" + " OR ".join(groups_or) + ")", binds


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


def assemble_sql(table: str, where_parts: List[str], order_by: str | None) -> str:
    where_sql = " AND ".join([part for part in where_parts if part and part.strip()])
    if not where_sql:
        where_sql = "1=1"
    if not order_by:
        default_order = get_setting_value("DW_DATE_COLUMN", scope="namespace") or "REQUEST_DATE"
        order_by = f"{default_order} DESC"
    return f"{build_select_all(table)}\nWHERE {where_sql}\nORDER BY {order_by}"


def run_rate(
    comment: str,
    table: str = "Contract",
    *,
    inquiry_id: Any | None = None,
    sample_rows: int | None = None,
) -> Dict[str, object]:
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
        where_eq, binds = build_eq_condition(table, col, vals, binds)
        wheres.append(where_eq)

    if fts_groups:
        where_fts, binds = build_fts_like(table, fts_groups, binds)
        wheres.append(where_fts)

    final_sql = assemble_sql(table, wheres, order_by)
    binds_copy = dict(binds)

    intent_debug = {
        "eq_filters": [{"col": col, "values": vals} for col, vals in eq_map.items()],
        "fts_groups": fts_groups,
        "sort_by": order_by or "REQUEST_DATE DESC",
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
        }
    )

    columns, rows, rc = _execute_and_sample(final_sql, binds_copy, sample_rows=sample_rows)

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
        },
    }

    return resp


__all__ = [
    "assemble_sql",
    "build_eq_condition",
    "build_select_all",
    "build_fts_like",
    "run_rate",
]
