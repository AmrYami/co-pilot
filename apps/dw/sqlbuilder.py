from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import re

from apps.dw.common.eq_aliases import resolve_eq_targets
from apps.dw.filters import build_boolean_groups_where
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


_SEGMENT_SPLIT_RE = re.compile(r"[\n;]+")


def _split_segments(comment: str) -> List[List[str]]:
    if not comment:
        return []
    fragments = [frag.strip() for frag in _SEGMENT_SPLIT_RE.split(comment) if frag.strip()]
    blocks: List[List[str]] = []
    current: List[str] = []
    for frag in fragments:
        if frag.lower() == "or":
            if current:
                blocks.append(current)
                current = []
            continue
        current.append(frag)
    if current:
        blocks.append(current)
    return blocks or [[]]


def _split_or_values(text: str) -> List[str]:
    tokens: List[str] = []
    for part in re.split(r"\s+or\s+|\s*\|\s*|,", text or "", flags=re.IGNORECASE):
        cleaned = part.strip().strip("\"'")
        if cleaned:
            tokens.append(cleaned)
    return tokens


def _parse_flag_blob(blob: str) -> Tuple[bool, bool]:
    ci = True
    trim = True
    if not blob:
        return ci, trim
    for raw in blob.split(","):
        token = raw.strip().lower()
        if not token:
            continue
        if token in {"ci", "case_insensitive"}:
            ci = True
        elif token in {"no_ci", "case_sensitive", "exact"}:
            ci = False
        elif token == "trim":
            trim = True
        elif token in {"no_trim", "raw"}:
            trim = False
    return ci, trim


def _add_eq_filter(
    eq_filters: List[Dict[str, Any]],
    column: str,
    values: List[str],
    *,
    op: str = "eq",
    ci: bool = True,
    trim: bool = True,
) -> None:
    if not column or not values:
        return
    norm_col = column.strip().upper()
    key = (norm_col, op, ci, trim)
    for existing in eq_filters:
        existing_key = (
            str(existing.get("col") or "").upper(),
            str(existing.get("op") or "eq"),
            bool(existing.get("ci", True)),
            bool(existing.get("trim", True)),
        )
        if existing_key == key:
            seen: set[str] = set()
            merged: List[str] = []
            for value in (existing.get("values") or []) + values:
                if value is None:
                    continue
                text = value.strip() if isinstance(value, str) else str(value)
                if not text or text in seen:
                    continue
                seen.add(text)
                merged.append(text)
            if merged:
                existing["values"] = merged
                existing["val"] = merged[0]
            return
    cleaned: List[str] = []
    seen_values: set[str] = set()
    for value in values:
        if value is None:
            continue
        text = value.strip() if isinstance(value, str) else str(value)
        if not text or text in seen_values:
            continue
        seen_values.add(text)
        cleaned.append(text)
    if not cleaned:
        return
    eq_filters.append(
        {
            "col": norm_col,
            "values": cleaned,
            "val": cleaned[0],
            "op": op,
            "ci": ci,
            "trim": trim,
        }
    )


def _parse_fts_segment(value: str) -> Tuple[List[str], str]:
    body = value or ""
    if "&" in body and "|" not in body and not re.search(r"\bor\b", body, flags=re.IGNORECASE):
        tokens = [frag.strip().strip("\"'") for frag in body.split("&")]
        operator = "AND"
    elif re.search(r"\band\b", body, flags=re.IGNORECASE) and not re.search(
        r"\bor\b", body, flags=re.IGNORECASE
    ):
        tokens = [frag.strip().strip("\"'") for frag in re.split(r"\band\b", body, flags=re.IGNORECASE)]
        operator = "AND"
    else:
        tokens = [frag.strip().strip("\"'") for frag in _split_or_values(body)]
        operator = "OR"
    cleaned = [tok for tok in tokens if tok]
    return cleaned, operator


def parse_rate_comment(comment: str) -> Dict[str, Any]:
    """Parse a /dw/rate comment into structured hints."""

    hints = {
        "fts_tokens": [],
        "fts_operator": "OR",
        "eq_filters": [],
        "boolean_groups": [],
        "group_by": None,
        "gross": None,
        "sort_by": None,
        "sort_desc": None,
        "top_n": None,
        "direction_hint": None,
    }
    if not comment:
        return hints

    for block in _split_segments(comment):
        for segment in block:
            if ":" not in segment:
                continue
            key, value = segment.split(":", 1)
            directive = key.strip().lower()
            payload = value.strip()
            if directive == "fts":
                tokens, operator = _parse_fts_segment(payload)
                if tokens:
                    hints["fts_tokens"] = tokens
                    hints["fts_operator"] = operator
                continue
            if directive in {"eq", "has", "have", "contains"}:
                if "=" not in payload:
                    continue
                col_raw, rhs = payload.split("=", 1)
                column = col_raw.strip()
                flag_match = re.search(r"\(([^)]*)\)\s*$", rhs)
                ci = True
                trim = True
                if flag_match:
                    ci, trim = _parse_flag_blob(flag_match.group(1))
                    rhs = rhs[: flag_match.start()].rstrip()
                op = "like" if directive in {"has", "have", "contains"} else "eq"
                values = _split_or_values(rhs)
                _add_eq_filter(hints["eq_filters"], column, values, op=op, ci=ci, trim=trim)
                continue
            if directive == "group_by":
                hints["group_by"] = _norm(payload.upper())
                continue
            if directive == "order_by":
                match = re.match(r"(.+?)\s+(asc|desc)$", payload, flags=re.IGNORECASE)
                if match:
                    hints["sort_by"] = _norm(match.group(1).upper())
                    hints["sort_desc"] = match.group(2).lower() == "desc"
                else:
                    hints["sort_by"] = _norm(payload.upper())
                continue
            if directive == "gross":
                lowered = payload.lower()
                if lowered in {"true", "false"}:
                    hints["gross"] = lowered == "true"
                continue
            if directive == "top_n":
                try:
                    hints["top_n"] = int(payload)
                except ValueError:
                    pass

    eq_filters = hints.get("eq_filters") or []
    if eq_filters:
        fields: List[Dict[str, Any]] = []
        for entry in eq_filters:
            column = str(entry.get("col") or "").strip()
            if not column:
                continue
            values = list(entry.get("values") or [])
            if not values:
                fallback = entry.get("val")
                if fallback:
                    values = [fallback]
            if not values:
                continue
            op = str(entry.get("op") or "eq").lower()
            fields.append({"field": column, "op": "like" if op == "like" else "eq", "values": values})
        if fields:
            hints["boolean_groups"] = [{"id": "A", "fields": fields}]

    text = comment.strip()
    match = re.search(
        r"\b(top|bottom|lowest|highest|smallest|largest|cheapest|min|max)\s+(\d+)\b",
        text,
        flags=re.IGNORECASE,
    )
    if match:
        hints["direction_hint"] = match.group(1).lower()
        hints["top_n"] = int(match.group(2))
    else:
        match2 = re.search(
            r"\b(top|bottom|lowest|highest|smallest|largest|cheapest|min|max)\b",
            text,
            flags=re.IGNORECASE,
        )
        if match2:
            hints["direction_hint"] = match2.group(1).lower()

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
    allow = {c.upper() for c in (allowed_columns or [])}

    grouped: Dict[Tuple[str, str, bool, bool], List[Any]] = {}
    for entry in eq_filters or []:
        column = _norm(str(entry.get("col") or "").upper())
        if not column:
            continue
        op = str(entry.get("op") or "eq").lower()
        if op not in {"eq", "like"}:
            op = "eq"
        ci = bool(entry.get("ci", True))
        trim = bool(entry.get("trim", True))
        values = entry.get("values") or []
        if not values:
            fallback = entry.get("val")
            if fallback not in (None, ""):
                values = [fallback]
        cleaned: List[Any] = []
        seen: set[Any] = set()
        for value in values:
            if value is None:
                continue
            candidate = value
            if isinstance(candidate, str):
                candidate = candidate.strip()
            if candidate == "":
                continue
            key = candidate.lower() if (ci and isinstance(candidate, str)) else candidate
            if key in seen:
                continue
            seen.add(key)
            if op == "like" and isinstance(candidate, str) and not (candidate.startswith("%") or candidate.endswith("%")):
                candidate = f"%{candidate}%"
            cleaned.append(candidate)
        if not cleaned:
            continue
        grouped.setdefault((column, op, ci, trim), []).extend(cleaned)

    bind_index = 0
    for (column, op, ci, trim), values in grouped.items():
        targets = resolve_eq_targets(column) or [column]
        resolved = [col for col in targets if not allow or col.upper() in allow]
        if not resolved:
            continue
        ors: List[str] = []
        for value in values:
            bind_name = f"eq_{bind_index}"
            bind_index += 1
            binds[bind_name] = value
            for target in resolved:
                left = target
                right = f":{bind_name}"
                if trim:
                    left = f"TRIM({left})"
                    right = f"TRIM({right})"
                if ci:
                    left = f"UPPER({left})"
                    right = f"UPPER({right})"
                operator = "LIKE" if op == "like" else "="
                ors.append(f"{left} {operator} {right}")
        if ors:
            parts.append("(" + " OR ".join(ors) + ")")

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
    boolean_groups = intent.get("boolean_groups") or []
    group_by = intent.get("group_by")
    gross = intent.get("gross")
    sort_by = intent.get("sort_by")
    sort_desc = intent.get("sort_desc")
    top_n = intent.get("top_n")
    direction_hint = intent.get("direction_hint")

    if (direction_hint is not None) and (sort_desc is None):
        sort_desc, note = direction_from_words([direction_hint])
        dbg["notes"].append(note)

    date_column = str(settings.get("DW_DATE_COLUMN") if isinstance(settings, dict) else "REQUEST_DATE")
    if not date_column or date_column.strip() == "{}":
        date_column = "REQUEST_DATE"
    date_column = date_column.strip()
    if sort_by is None:
        sort_by = date_column
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

    bg_where_sql = ""
    bg_binds: Dict[str, Any] = {}
    if boolean_groups:
        bg_where_sql, bg_binds = build_boolean_groups_where(boolean_groups, settings)
    elif eq_filters:
        fallback_fields: List[Dict[str, Any]] = []
        for entry in eq_filters:
            column = str(entry.get("col") or "").strip()
            if not column:
                continue
            values = list(entry.get("values") or [])
            if not values and entry.get("val"):
                values = [entry["val"]]
            if not values:
                continue
            op = str(entry.get("op") or "eq").lower()
            fallback_fields.append(
                {"field": column, "op": "like" if op == "like" else "eq", "values": values}
            )
        if fallback_fields:
            bg_where_sql, bg_binds = build_boolean_groups_where(
                [{"id": "A", "fields": fallback_fields}],
                settings,
            )
    if bg_where_sql:
        where_parts.append(bg_where_sql)
        binds.update(bg_binds)
        dbg["notes"].append(f"boolean_groups={len(bg_binds)}")

    where_clause = ""
    if where_parts:
        where_clause = "WHERE " + " AND ".join([p for p in where_parts if p])

    sort_by_text = str(sort_by or date_column).strip()
    if not sort_by_text:
        sort_by_text = date_column
    upper_sort = sort_by_text.upper()
    if upper_sort.endswith(" DESC") or upper_sort.endswith(" ASC"):
        sort_by_text = sort_by_text.rsplit(" ", 1)[0]
    sort_by_text = sort_by_text.replace("_DESC", "").strip() or date_column
    order_clause = f"ORDER BY {sort_by_text} {'DESC' if sort_desc else 'ASC'}"

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
