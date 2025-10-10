from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import re

from core.nlu.schema import NLIntent


def overlap_predicate(strict: bool = True) -> str:
    """Return the contract overlap predicate with optional NULL tolerance."""

    if strict:
        return "(START_DATE <= :date_end AND END_DATE >= :date_start)"
    return (
        "((START_DATE IS NULL OR START_DATE <= :date_end) "
        "AND (END_DATE IS NULL OR END_DATE >= :date_start))"
    )


def select_all(table: str) -> str:
    return f'SELECT * FROM "{table}"'


def order_limit(order_sql: str | None, top_n: int | None) -> str:
    parts: List[str] = []
    if order_sql:
        parts.append(f"ORDER BY {order_sql}")
    if top_n:
        parts.append("FETCH FIRST :top_n ROWS ONLY")
    return "\n".join(parts)


def _intent_dates(intent: NLIntent) -> tuple[Optional[str], Optional[str]]:
    window = getattr(intent, "explicit_dates", None)
    if window is None:
        return None, None
    start = getattr(window, "start", None)
    end = getattr(window, "end", None)
    if start and end:
        return str(start), str(end)
    return None, None


def _window_clause(intent: NLIntent, alias: str = "") -> tuple[str, Dict[str, Any]]:
    start, end = _intent_dates(intent)
    if not start or not end:
        return "", {}
    column = intent.date_column or "REQUEST_DATE"
    col_expr = f"{alias}{column}"
    return f"WHERE {col_expr} BETWEEN :date_start AND :date_end", {
        "date_start": start,
        "date_end": end,
    }


def build_dw_sql(
    intent: NLIntent,
    table: str = '"Contract"',
    select_all_default: bool = True,
    auto_detail: bool = True,
) -> Optional[dict]:
    """Return a deterministic SQL payload or None when insufficient intent."""

    has_window = bool(_intent_dates(intent)[0] and _intent_dates(intent)[1])
    wants_topn = bool(intent.top_n)
    grouped = bool(intent.group_by)
    counting = intent.agg == "count"

    if not (has_window or wants_topn or grouped or counting):
        return None

    binds: Dict[str, Any] = {}
    where, window_binds = _window_clause(intent)
    binds.update(window_binds)

    if counting and not grouped:
        sql = f"SELECT COUNT(*) AS CNT FROM {table} {where}".strip()
        return {"sql": sql, "binds": binds, "detail": False}

    if grouped:
        dim = intent.group_by
        measure = intent.measure_sql or "NVL(CONTRACT_VALUE_NET_OF_VAT,0)"
        agg_sql = f"SUM({measure})"
        summary_lines = [
            f"SELECT {dim} AS GROUP_KEY, {agg_sql} AS MEASURE",
            f"FROM {table}",
        ]
        if where:
            summary_lines.append(where)
        summary_lines.append(f"GROUP BY {dim}")
        summary_lines.append("ORDER BY MEASURE DESC")
        if intent.top_n:
            summary_lines.append("FETCH FIRST :top_n ROWS ONLY")
            binds["top_n"] = intent.top_n
        summary_sql = "\n".join(summary_lines)
        result: Dict[str, Any] = {"sql": summary_sql, "binds": dict(binds), "detail": False}

        if auto_detail and intent.top_n:
            detail_lines = [
                "WITH top_dim AS (",
                f"  SELECT {dim} AS GROUP_KEY, {agg_sql} AS MEASURE",
                f"  FROM {table}",
            ]
            if where:
                detail_lines.append(f"  {where}")
            detail_lines.extend(
                [
                    f"  GROUP BY {dim}",
                    f"  ORDER BY MEASURE DESC",
                    f"  FETCH FIRST :top_n ROWS ONLY",
                    ")",
                    f"SELECT c.*",
                    f"FROM {table} c",
                    f"JOIN top_dim t ON c.{dim} = t.GROUP_KEY",
                ]
            )
            if where:
                detail_where = where.replace("WHERE ", "WHERE c.", 1)
                detail_lines.append(detail_where)
            detail_lines.append(f"ORDER BY t.MEASURE DESC, c.{intent.date_column} DESC")
            result["detail_sql"] = "\n".join(detail_lines)
            result["detail"] = True
        return result

    projection = "*" if select_all_default else (
        f"CONTRACT_ID, CONTRACT_OWNER, {intent.date_column} AS WINDOW_DATE, "
        f"{intent.measure_sql or 'NVL(CONTRACT_VALUE_NET_OF_VAT,0)'} AS VALUE"
    )
    lines = [
        f"SELECT {projection}",
        f"FROM {table}",
    ]
    if where:
        lines.append(where)
    if intent.top_n:
        order_by = intent.sort_by or intent.date_column or "REQUEST_DATE"
        direction = "DESC" if intent.sort_desc else "ASC"
        lines.append(f"ORDER BY {order_by} {direction}")
        lines.append("FETCH FIRST :top_n ROWS ONLY")
        binds["top_n"] = intent.top_n
    sql = "\n".join(lines)
    return {"sql": sql, "binds": binds, "detail": False}


GROSS_EXPR = (
    "NVL(CONTRACT_VALUE_NET_OF_VAT,0) + "
    "CASE WHEN NVL(VAT,0) BETWEEN 0 AND 1 "
    "THEN NVL(CONTRACT_VALUE_NET_OF_VAT,0) * NVL(VAT,0) "
    "ELSE NVL(VAT,0) END"
)

TOP_WORDS_DESC = {"top", "highest", "largest", "biggest", "max"}
BOTTOM_WORDS_ASC = {"bottom", "lowest", "smallest", "cheapest", "min"}


def _norm(s: str) -> str:
    return (s or "").strip()


def parse_rate_comment(comment: str) -> Dict[str, Any]:
    """Parse a /dw/rate comment into structured hints."""

    hints = {
        "fts_tokens": [],
        "fts_operator": "OR",
        "eq_filters": [],
        "group_by": None,
        "gross": None,
        "sort_by": None,
        "sort_desc": None,
        "top_n": None,
        "direction_hint": None,
    }
    if not comment:
        return hints

    text = comment.strip()

    fts_and = re.search(r"fts\s*\(\s*and\s*\)\s*:\s*([^;]+)", text, flags=re.I)
    fts_or = re.search(r"fts\s*:\s*([^;]+)", text, flags=re.I)
    if fts_and:
        tokens = re.split(r"[&]", fts_and.group(1))
        hints["fts_tokens"] = [t.strip() for t in tokens if t.strip()]
        hints["fts_operator"] = "AND"
    elif fts_or:
        tokens = re.split(r"[|]", fts_or.group(1))
        hints["fts_tokens"] = [t.strip() for t in tokens if t.strip()]
        hints["fts_operator"] = "OR"

    for m in re.finditer(r"eq\s*:\s*([A-Za-z0-9_]+)\s*=\s*([^;]+)", text, flags=re.I):
        col = _norm(m.group(1).upper())
        val_raw = _norm(m.group(2))
        val = val_raw
        ci = True
        trim = True
        flag_m = re.search(r"\(([^)]+)\)$", val_raw)
        if flag_m:
            val = _norm(val_raw[: flag_m.start()])
            flags = {f.strip().lower() for f in flag_m.group(1).split(",")}
            if {"cs", "case_sensitive", "no_ci", "exact"} & flags:
                ci = False
            if {"raw", "no_trim"} & flags:
                trim = False
            if {"ci", "case_insensitive"} & flags:
                ci = True
            if "trim" in flags:
                trim = True
        if (len(val) >= 2) and ((val[0] in "\"'") and (val[-1] == val[0])):
            val = val[1:-1]
        hints["eq_filters"].append({"col": col, "val": val, "ci": ci, "trim": trim})

    m = re.search(r"group_by\s*:\s*([A-Za-z0-9_]+)", text, flags=re.I)
    if m:
        hints["group_by"] = _norm(m.group(1).upper())

    m = re.search(r"gross\s*:\s*(true|false)", text, flags=re.I)
    if m:
        hints["gross"] = m.group(1).lower() == "true"

    m = re.search(r"order_by\s*:\s*([A-Za-z0-9_]+)\s*(asc|desc)?", text, flags=re.I)
    if m:
        hints["sort_by"] = _norm(m.group(1).upper())
        hints["sort_desc"] = (m.group(2) or "DESC").strip().lower() == "desc"

    m = re.search(
        r"\b(top|bottom|lowest|highest|smallest|largest|cheapest|min|max)\s+(\d+)\b",
        text,
        flags=re.I,
    )
    if m:
        hints["direction_hint"] = m.group(1).lower()
        hints["top_n"] = int(m.group(2))
    else:
        m2 = re.search(
            r"\b(top|bottom|lowest|highest|smallest|largest|cheapest|min|max)\b",
            text,
            flags=re.I,
        )
        if m2:
            hints["direction_hint"] = m2.group(1).lower()

    return hints


def _fts_like_group(columns: List[str], token: str, bind_name: str) -> Tuple[str, Dict[str, Any]]:
    or_parts = [f"UPPER(NVL({c},'')) LIKE UPPER(:{bind_name})" for c in columns]
    clause = "(" + " OR ".join(or_parts) + ")"
    return clause, {bind_name: f"%{token}%"}


def build_fts_where_like(
    columns: List[str], tokens: List[str], op: str
) -> Tuple[str, Dict[str, Any]]:
    op = op.upper() if op else "OR"
    parts: List[str] = []
    binds: Dict[str, Any] = {}
    for i, tok in enumerate(tokens):
        if not tok:
            continue
        name = f"fts_{i}"
        gp, b = _fts_like_group(columns, tok, name)
        parts.append(gp)
        binds.update(b)
    if not parts:
        return "", {}
    glue = f" {op} "
    return "(" + glue.join(parts) + ")", binds


def build_eq_where(
    eq_filters: List[Dict[str, Any]], allowed_columns: List[str]
) -> Tuple[str, Dict[str, Any]]:
    parts: List[str] = []
    binds: Dict[str, Any] = {}
    idx = 0
    allow = {c.upper() for c in (allowed_columns or [])}
    for f in eq_filters or []:
        col = _norm(str(f.get("col") or "").upper())
        if not col or (allow and col not in allow):
            continue
        op = str(f.get("op") or "eq").lower()
        val = f.get("val", "")
        ci = bool(f.get("ci", True))
        trim = bool(f.get("trim", True))
        bname = f"eq_{idx}"
        left_expr = col
        if trim:
            left_expr = f"TRIM({left_expr})"
        if op == "like":
            pattern = val
            if isinstance(pattern, str) and pattern and not pattern.startswith("%") and not pattern.endswith("%"):
                pattern = f"%{pattern}%"
            binds[bname] = pattern
            right_expr = f":{bname}"
            if trim:
                right_expr = f"TRIM({right_expr})"
            if ci:
                parts.append(f"UPPER({left_expr}) LIKE UPPER({right_expr})")
            else:
                parts.append(f"{left_expr} LIKE {right_expr}")
            idx += 1
            continue
        if ci:
            left_cmp = f"UPPER({left_expr})"
        else:
            left_cmp = left_expr
        right_expr = f":{bname}"
        if trim:
            right_expr = f"TRIM({right_expr})"
        if ci:
            right_cmp = f"UPPER({right_expr})"
        else:
            right_cmp = right_expr
        parts.append(f"{left_cmp} = {right_cmp}")
        binds[bname] = val
        idx += 1
    where = ("(" + " AND ".join(parts) + ")") if parts else ""
    return where, binds


def direction_from_words(words: List[str]) -> Tuple[bool, str]:
    s = {w.lower() for w in words or []}
    if s & TOP_WORDS_DESC:
        return True, "desc_from_top_words"
    if s & BOTTOM_WORDS_ASC:
        return False, "asc_from_bottom_words"
    return True, "default_desc"


def build_sql_from_intent(
    intent: Dict[str, Any],
    settings: Dict[str, Any],
    table: str = "Contract",
) -> Tuple[str, Dict[str, Any], Dict[str, Any]]:
    dbg = {"notes": []}
    binds: Dict[str, Any] = {}

    fts_cfg = settings.get("DW_FTS_COLUMNS") or {}
    fts_cols = fts_cfg.get(table) or fts_cfg.get(table.upper())
    if not fts_cols:
        fts_cols = fts_cfg.get("*") or []
    eq_allowed = settings.get("DW_EXPLICIT_FILTER_COLUMNS") or []
    fts_engine = (settings.get("DW_FTS_ENGINE") or "like").strip().lower()

    fts_tokens = intent.get("fts_tokens") or []
    fts_operator = (intent.get("fts_operator") or "OR").upper()
    full_text_search = bool(intent.get("full_text_search") or (fts_tokens and fts_engine == "like"))
    eq_filters = intent.get("eq_filters") or []
    group_by = intent.get("group_by")
    gross = intent.get("gross")
    sort_by = intent.get("sort_by")
    sort_desc = intent.get("sort_desc")
    top_n = intent.get("top_n")
    direction_hint = intent.get("direction_hint")

    if (direction_hint is not None) and (sort_desc is None):
        sort_desc, note = direction_from_words([direction_hint])
        dbg["notes"].append(note)

    if sort_by is None:
        sort_by = "REQUEST_DATE"
    if sort_desc is None:
        sort_desc = True

    select_clause = "*"
    from_clause = f'FROM "{table}"'
    where_parts: List[str] = []

    if full_text_search and fts_engine == "like" and fts_cols and fts_tokens:
        w, b = build_fts_where_like(fts_cols, fts_tokens, fts_operator)
        if w:
            where_parts.append(w)
            binds.update(b)
            dbg["notes"].append(
                f"fts_like columns={len(fts_cols)} tokens={len(fts_tokens)} op={fts_operator}"
            )
    elif full_text_search and fts_engine != "like":
        dbg["notes"].append("unsupported_fts_engine_fallback_like_disabled")

    if eq_filters:
        w, b = build_eq_where(eq_filters, eq_allowed)
        if w:
            where_parts.append(w)
            binds.update(b)
            dbg["notes"].append(f"eq_filters={len(b)}")

    where_clause = ""
    if where_parts:
        where_clause = "WHERE " + " AND ".join([p for p in where_parts if p])

    order_clause = f"ORDER BY {sort_by} {'DESC' if sort_desc else 'ASC'}"

    if group_by:
        if gross:
            select_clause = f"{group_by} AS GROUP_KEY, {GROSS_EXPR} AS TOTAL_GROSS, COUNT(*) AS CNT"
            order_target = "TOTAL_GROSS"
        else:
            select_clause = f"{group_by} AS GROUP_KEY, COUNT(*) AS CNT"
            order_target = "CNT"
        group_clause = f"GROUP BY {group_by}"
        sql = (
            f"SELECT {select_clause} {from_clause} {where_clause} {group_clause} ORDER BY {order_target} "
            f"{'DESC' if sort_desc else 'ASC'}"
        )
    else:
        sql = f"SELECT {select_clause} {from_clause} {where_clause} {order_clause}"

    if isinstance(top_n, int) and top_n > 0:
        sql = f"{sql}\nFETCH FIRST {top_n} ROWS ONLY"

    parts = sql.split("ORDER BY")
    if len(parts) > 2:
        sql = "ORDER BY".join([parts[0]] + [" ".join(parts[-1].split())])

    return sql.strip(), binds, dbg
