from __future__ import annotations

from typing import Dict, List, Tuple

try:  # pragma: no cover - optional helper
    from apps.dw.utils import to_upper_trim as _external_to_upper_trim
except Exception:  # pragma: no cover - fallback when helper absent
    _external_to_upper_trim = None  # type: ignore[assignment]

from apps.dw.db import run_query
from apps.dw.logger import log
from apps.dw.settings import get_setting_json, get_setting_value


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


def assemble_sql(table: str, where_parts: List[str], order_by: str | None) -> str:
    where_sql = " AND ".join([part for part in where_parts if part and part.strip()])
    if not where_sql:
        where_sql = "1=1"
    if not order_by:
        default_order = get_setting_value("DW_DATE_COLUMN", scope="namespace") or "REQUEST_DATE"
        order_by = f"{default_order} DESC"
    return f'SELECT * FROM "{table}"
WHERE {where_sql}
ORDER BY {order_by}'


def run_rate(comment: str, table: str = "Contract") -> Dict[str, object]:
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

    sql = assemble_sql(table, wheres, order_by)

    log.debug("rate_pipeline.sql", extra={"sql": sql, "binds": binds}) if hasattr(log, "debug") else None
    rows = run_query(sql, binds)

    debug = {
        "intent": {
            "eq_filters": [{"col": col, "values": vals} for col, vals in eq_map.items()],
            "fts_groups": fts_groups,
            "sort_by": order_by or "REQUEST_DATE DESC",
        },
        "final_sql": {"sql": sql, "size": len(sql)},
        "validation": {
            "ok": True,
            "bind_names": list(binds.keys()),
            "binds": dict(binds),
            "errors": [],
        },
    }
    return {"sql": sql, "binds": binds, "rows": rows, "debug": debug}


__all__ = [
    "assemble_sql",
    "build_eq_condition",
    "build_fts_like",
    "run_rate",
]
