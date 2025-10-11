from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import re

from apps.dw.common.eq_aliases import resolve_eq_targets
from apps.dw.filters import build_boolean_groups_where
from apps.dw.search.fts_registry import resolve_engine
from apps.dw.sql.builder import QueryBuilder
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

    def _intent_get(key: str, default=None):
        if isinstance(intent, dict):
            return intent.get(key, default)
        return getattr(intent, key, default)

    fts_cfg = settings.get("DW_FTS_COLUMNS") or {}
    fts_cols = fts_cfg.get(table) or fts_cfg.get(table.upper()) or fts_cfg.get("*") or []
    fts_engine_name = (settings.get("DW_FTS_ENGINE") or "like").strip().lower()
    fts_engine = resolve_engine(fts_engine_name)
    min_token_len = settings.get("DW_FTS_MIN_TOKEN_LEN", 2)

    fts_tokens = _intent_get("fts_tokens") or []
    fts_groups = _intent_get("fts_groups") or [[tok] for tok in fts_tokens]
    fts_operator = (_intent_get("fts_operator") or "OR").upper()
    full_text_search = bool(_intent_get("full_text_search") and fts_cols and fts_engine)

    boolean_groups = _intent_get("boolean_groups") or []
    eq_filters = _intent_get("eq_filters") or []
    group_by = _intent_get("group_by")
    gross = _intent_get("gross")
    sort_by = _intent_get("sort_by")
    sort_desc = _intent_get("sort_desc")
    top_n = _intent_get("top_n")
    direction_hint = _intent_get("direction_hint")
    wants_all = _intent_get("wants_all_columns", True)

    if (direction_hint is not None) and (sort_desc is None):
        sort_desc, note = direction_from_words([direction_hint])
        dbg["notes"].append(note)

    date_column = str(settings.get("DW_DATE_COLUMN") or "REQUEST_DATE").strip() or "REQUEST_DATE"
    if sort_desc is None:
        sort_desc = True

    qb = QueryBuilder(table=table, date_col=date_column)
    qb.wants_all_columns(bool(wants_all))

    if full_text_search and fts_cols and fts_groups:
        qb.use_fts(engine=fts_engine, columns=fts_cols, min_token_len=min_token_len)
        for group in fts_groups:
            qb.add_fts_group(group, op=fts_operator)

    if boolean_groups:
        qb.apply_boolean_groups(boolean_groups)
    elif eq_filters:
        qb.apply_eq_filters(eq_filters)

    if group_by:
        qb.group_by(group_by if isinstance(group_by, list) else [group_by], gross=bool(gross))

    order_column = sort_by or ("TOTAL_GROSS" if (group_by and gross) else ("CNT" if group_by else date_column))
    qb.order_by(order_column, desc=bool(sort_desc))
    qb.limit(top_n)

    sql, binds = qb.compile()
    dbg_notes = qb.debug_info().get("notes") or []
    dbg["notes"].extend(dbg_notes)
    return sql.strip(), binds, dbg
