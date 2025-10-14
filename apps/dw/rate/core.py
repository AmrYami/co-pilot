# -*- coding: utf-8 -*-
"""Core parser and SQL builder for the ``/dw/rate`` endpoint.

This module provides a lightweight intent model that can be shared by tests
and by the HTTP blueprint.  It intentionally mirrors the configuration keys
used throughout the DW stack so the behaviour matches the existing
``/dw/answer`` implementation (FTS columns, enum synonyms, aliases, etc.).
"""

from __future__ import annotations

import datetime as dt
import re
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Tuple


# ---------------------------------------------------------------------------
# Intent model
# ---------------------------------------------------------------------------


@dataclass
class RateIntent:
    """Structured representation of a parsed ``/dw/rate`` comment."""

    fts_groups: List[List[str]] = field(default_factory=list)
    eq_filters: List[Tuple[str, List[str]]] = field(default_factory=list)
    neq_filters: List[Tuple[str, List[str]]] = field(default_factory=list)
    contains: List[Tuple[str, List[str]]] = field(default_factory=list)
    not_contains: List[Tuple[str, List[str]]] = field(default_factory=list)
    empty_any: List[List[str]] = field(default_factory=list)
    empty_all: List[List[str]] = field(default_factory=list)
    not_empty: List[str] = field(default_factory=list)
    numeric: List[Tuple[str, str, List[float]]] = field(default_factory=list)
    order_by: List[Tuple[str, str]] = field(default_factory=list)
    limit: Optional[int] = None
    offset: Optional[int] = None
    when_kind: Optional[str] = None  # requested | active | expiring
    date_start: Optional[dt.date] = None
    date_end: Optional[dt.date] = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

IDENT = r"[A-Za-z0-9_\.]+"
VALUE = r"[^;]+"


def _to_int(raw: str) -> Optional[int]:
    try:
        return int(raw)
    except Exception:
        return None


def _start_of_month(date_value: dt.date) -> dt.date:
    return date_value.replace(day=1)


def _end_of_month(date_value: dt.date) -> dt.date:
    next_month = (date_value.replace(day=28) + dt.timedelta(days=4)).replace(day=1)
    return next_month - dt.timedelta(days=1)


def _quarter_bounds(date_value: dt.date) -> Tuple[dt.date, dt.date]:
    quarter = (date_value.month - 1) // 3 + 1
    start_month = 3 * (quarter - 1) + 1
    start = dt.date(date_value.year, start_month, 1)
    end_month = start_month + 2
    end = _end_of_month(dt.date(date_value.year, end_month, 1))
    return start, end


def _shift_months(date_value: dt.date, months: int) -> dt.date:
    year = date_value.year + (date_value.month - 1 + months) // 12
    month = (date_value.month - 1 + months) % 12 + 1
    end_day = _end_of_month(dt.date(year, month, 1)).day
    day = min(date_value.day, end_day)
    return dt.date(year, month, day)


def _expand_request_type(values: Iterable[str], settings: Dict[str, object]) -> Tuple[List[str], List[str], List[str]]:
    """Return (equals, prefix, contains) expansions using DW_ENUM_SYNONYMS."""

    enum_cfg = settings.get("DW_ENUM_SYNONYMS") if isinstance(settings, dict) else {}
    if not isinstance(enum_cfg, dict):
        enum_cfg = {}

    # Synonyms can be stored under "Contract.REQUEST_TYPE" or using a custom table name.
    table = str(settings.get("DW_CONTRACT_TABLE", "Contract") or "Contract")
    syn_key_candidates = [
        f"{table}.REQUEST_TYPE",
        f"{table.upper()}.REQUEST_TYPE",
        "Contract.REQUEST_TYPE",
        "CONTRACT.REQUEST_TYPE",
        "REQUEST_TYPE",
    ]
    syn_map: Dict[str, object] = {}
    for key in syn_key_candidates:
        if isinstance(enum_cfg.get(key), dict):
            syn_map = enum_cfg[key]  # type: ignore[assignment]
            break

    equals: List[str] = []
    prefixes: List[str] = []
    contains_tokens: List[str] = []
    if not syn_map:
        equals = [v.upper() for v in values if isinstance(v, str) and v.strip()]
        return equals, prefixes, contains_tokens

    normalized_values = {str(v).strip().upper() for v in values if isinstance(v, str) and v.strip()}
    def _iterable_values(obj: object) -> Iterable[str]:
        if isinstance(obj, (list, tuple, set)):
            return [str(item) for item in obj if isinstance(item, str)]
        return []

    for raw_key, spec in syn_map.items():
        if not isinstance(spec, dict):
            continue
        key_upper = str(raw_key).strip().upper()
        equals_group = {key_upper}
        for eq_val in _iterable_values(spec.get("equals")):
            if eq_val.strip():
                equals_group.add(eq_val.strip().upper())
        if normalized_values & equals_group:
            equals.extend(sorted(equals_group))
            for prefix in _iterable_values(spec.get("prefix")):
                if prefix.strip():
                    prefixes.append(prefix.strip().upper())
            for token in _iterable_values(spec.get("contains")):
                if token.strip():
                    contains_tokens.append(token.strip().upper())
    if not equals:
        equals = list(normalized_values)
    return equals, prefixes, contains_tokens


# ---------------------------------------------------------------------------
# Date parsing (English & Arabic keywords)
# ---------------------------------------------------------------------------


def parse_time_window(comment: str) -> Tuple[Optional[str], Optional[dt.date], Optional[dt.date]]:
    """Extract a (kind, start, end) window from ``comment`` if present."""

    text = (comment or "").lower()
    today = dt.date.today()

    when_kind: Optional[str] = None
    if re.search(r"\brequested\b|\brequested\s+(last|next)\b|تم\s+الطلب|الطلبات", text):
        when_kind = "requested"
    elif re.search(r"\bexpir\w*\b|تنتهي|سينتهي|قرب الانتهاء", text):
        when_kind = "expiring"
    else:
        when_kind = "active"

    match = re.search(r"\b(last|previous)\s+quarter\b|الربع\s+السابق", text)
    if match:
        this_q_start, _ = _quarter_bounds(today)
        end = this_q_start - dt.timedelta(days=1)
        start, end = _quarter_bounds(end)
        return when_kind, start, end

    match = re.search(r"\bnext\s+quarter\b|الربع\s+القادم", text)
    if match:
        _, this_q_end = _quarter_bounds(today)
        next_start = this_q_end + dt.timedelta(days=1)
        start, end = _quarter_bounds(next_start)
        return when_kind, start, end

    match = re.search(
        r"\blast\s+(\d+)\s+(day|days|week|weeks|month|months|year|years)\b|"
        r"آخر\s+(\d+)\s+(يوم|أيام|أسبوع|أسابيع|شهر|شهور|سنة|سنوات)",
        text,
    )
    if match:
        count = int(match.group(1) or match.group(3))
        unit = (match.group(2) or match.group(4) or "days").lower()
        end = today
        if unit.startswith("day") or unit in {"يوم", "أيام"}:
            start = today - dt.timedelta(days=count)
        elif unit.startswith("week") or unit in {"أسبوع", "أسابيع"}:
            start = today - dt.timedelta(days=7 * count)
        elif unit.startswith("month") or unit in {"شهر", "شهور"}:
            start = _shift_months(today, -count)
        else:
            start = dt.date(today.year - count, today.month, today.day)
        return when_kind, start, end

    match = re.search(
        r"\bnext\s+(\d+)\s+(day|days|week|weeks|month|months|year|years)\b|"
        r"القادم(?:ة)?\s+(\d+)\s+(يوم|أيام|أسبوع|أسابيع|شهر|شهور|سنة|سنوات)",
        text,
    )
    if match:
        count = int(match.group(1) or match.group(3))
        unit = (match.group(2) or match.group(4) or "days").lower()
        start = today
        if unit.startswith("day") or unit in {"يوم", "أيام"}:
            end = today + dt.timedelta(days=count)
        elif unit.startswith("week") or unit in {"أسبوع", "أسابيع"}:
            end = today + dt.timedelta(days=7 * count)
        elif unit.startswith("month") or unit in {"شهر", "شهور"}:
            end = _shift_months(today, count)
        else:
            end = dt.date(today.year + count, today.month, today.day)
        return when_kind, start, end

    match = re.search(
        r"\bbetween\b\s*(\d{4}-\d{2}-\d{2})\s*(?:and|-|to)\s*(\d{4}-\d{2}-\d{2})|"
        r"\bبين\s*(\d{4}-\d{2}-\d{2})\s*(?:و|-|إلى|الى)\s*(\d{4}-\d{2}-\d{2})",
        text,
    )
    if match:
        first = match.group(1) or match.group(3)
        second = match.group(2) or match.group(4)
        if first and second:
            start = dt.date.fromisoformat(first)
            end = dt.date.fromisoformat(second)
            return when_kind, start, end

    match = re.search(r"\bfrom\s+(\d{4}-\d{2}-\d{2})|\bمن\s+(\d{4}-\d{2}-\d{2})", text)
    if match:
        first = match.group(1) or match.group(2)
        start = dt.date.fromisoformat(first)
        return when_kind, start, today

    if re.search(r"\blast\s+month\b|الشهر\s+الماضي", text):
        end = _start_of_month(today) - dt.timedelta(days=1)
        start = end.replace(day=1)
        return when_kind, start, end

    if re.search(r"\bnext\s+month\b|الشهر\s+القادم", text):
        next_first = _shift_months(_start_of_month(today), 1)
        return when_kind, next_first, _end_of_month(next_first)

    if re.search(r"\blast\s+year\b|السنة\s+الماضية", text):
        start = dt.date(today.year - 1, 1, 1)
        end = dt.date(today.year - 1, 12, 31)
        return when_kind, start, end

    if re.search(r"\bnext\s+year\b|السنة\s+القادمة", text):
        start = dt.date(today.year + 1, 1, 1)
        end = dt.date(today.year + 1, 12, 31)
        return when_kind, start, end

    return None, None, None


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------


def parse_rate_comment(comment: str, settings: Dict[str, object] | None = None) -> RateIntent:
    """Parse ``comment`` into a :class:`RateIntent`."""

    text = (comment or "").strip()
    intent = RateIntent()

    for match in re.finditer(r"fts:\s*([^;]+)", text, flags=re.IGNORECASE):
        group = match.group(1)
        tokens: List[str] = []
        for quoted in re.findall(r'"([^"]+)"', group):
            tokens.append(quoted.strip())
            group = group.replace(f'"{quoted}"', "")
        for token in re.split(r"\bor\b|\band\b|\||أو|و", group, flags=re.IGNORECASE):
            if token.strip():
                tokens.append(token.strip())
        cleaned = [tok for tok in (tok.strip() for tok in tokens) if tok]
        if cleaned:
            intent.fts_groups.append(cleaned)

    pattern_with_optional_end = r"(?:;|$)"
    for regex, attr in (
        (rf"\beq:\s*({IDENT})\s*=\s*({VALUE}){pattern_with_optional_end}", "eq_filters"),
        (rf"\bneq:\s*({IDENT})\s*=\s*({VALUE}){pattern_with_optional_end}", "neq_filters"),
        (rf"\bcontains:\s*({IDENT})\s*=\s*({VALUE}){pattern_with_optional_end}", "contains"),
        (rf"\bnot_contains:\s*({IDENT})\s*=\s*({VALUE}){pattern_with_optional_end}", "not_contains"),
    ):
        for match in re.finditer(regex, text, flags=re.IGNORECASE):
            column = match.group(1).strip()
            raw_values = match.group(2)
            values = [val.strip() for val in re.split(r"\bor\b|\||أو", raw_values, flags=re.IGNORECASE) if val.strip()]
            getattr(intent, attr).append((column, values))

    empty_any_match = re.search(r"\bempty_any:\s*([^;]+)", text, flags=re.IGNORECASE)
    if empty_any_match:
        cols = [col.strip() for col in empty_any_match.group(1).split(",") if col.strip()]
        if cols:
            intent.empty_any.append(cols)

    for match in re.finditer(r"\bempty_all:\s*([^;]+)", text, flags=re.IGNORECASE):
        cols = [col.strip() for col in match.group(1).split(",") if col.strip()]
        if cols:
            intent.empty_all.append(cols)

    for match in re.finditer(r"\bnot_empty:\s*([^;]+)", text, flags=re.IGNORECASE):
        cols = [col.strip() for col in match.group(1).split(",") if col.strip()]
        intent.not_empty.extend(cols)

    for op in ("gt", "gte", "lt", "lte"):
        regex = rf"\b{op}:\s*({IDENT})\s*=\s*({VALUE}){pattern_with_optional_end}"
        for match in re.finditer(regex, text, flags=re.IGNORECASE):
            column = match.group(1).strip()
            raw_values = match.group(2)
            try:
                values = [float(value) for value in re.split(r"\bor\b|,|أو", raw_values, flags=re.IGNORECASE) if value.strip()]
            except ValueError:
                continue
            intent.numeric.append((column, op, values))

    between_pattern = (
        rf"\bbetween:\s*({IDENT})\s*=\s*(\d+(?:\.\d+)?)\s*(?:and|-|to)\s*(\d+(?:\.\d+)?){pattern_with_optional_end}"
    )
    for match in re.finditer(between_pattern, text, flags=re.IGNORECASE):
        column = match.group(1).strip()
        start = float(match.group(2))
        end = float(match.group(3))
        intent.numeric.append((column, "between", [start, end]))

    order_match = re.search(r"\border_by:\s*([^;]+)", text, flags=re.IGNORECASE)
    if order_match:
        parts = [part.strip() for part in order_match.group(1).split(",") if part.strip()]
        for part in parts:
            tokens = part.split()
            column = tokens[0].strip()
            direction = tokens[1].lower() if len(tokens) > 1 else "desc"
            intent.order_by.append((column, "asc" if direction.startswith("asc") else "desc"))

    limit_match = re.search(r"\blimit:\s*(\d+)", text, flags=re.IGNORECASE)
    if limit_match:
        intent.limit = _to_int(limit_match.group(1))

    offset_match = re.search(r"\boffset:\s*(\d+)", text, flags=re.IGNORECASE)
    if offset_match:
        intent.offset = _to_int(offset_match.group(1))

    when_kind, start, end = parse_time_window(text)
    intent.when_kind = when_kind
    intent.date_start = start
    intent.date_end = end

    return intent


# ---------------------------------------------------------------------------
# SQL builder
# ---------------------------------------------------------------------------


def build_sql(intent: RateIntent, settings: Dict[str, object]) -> Tuple[str, Dict[str, object]]:
    """Return an ``(sql, binds)`` pair for the provided intent."""

    table = str(settings.get("DW_CONTRACT_TABLE", "Contract") or "Contract")

    fts_cfg = settings.get("DW_FTS_COLUMNS", {}) if isinstance(settings, dict) else {}
    if isinstance(fts_cfg, dict):
        fts_cols = (
            fts_cfg.get(table)
            or fts_cfg.get(table.upper())
            or fts_cfg.get("*")
            or []
        )
    else:
        fts_cols = []
    fts_cols = [str(col).strip() for col in fts_cols if str(col).strip()]
    if not fts_cols:
        fts_cols = ["CONTRACT_SUBJECT", "CONTRACT_PURPOSE"]

    eq_alias = settings.get("DW_EQ_ALIAS_COLUMNS", {}) if isinstance(settings, dict) else {}
    if not isinstance(eq_alias, dict):
        eq_alias = {}

    binds: Dict[str, object] = {}
    where: List[str] = []

    def add_bind(prefix: str, value: object) -> str:
        index = 0
        key = prefix
        while key in binds:
            index += 1
            key = f"{prefix}_{index}"
        binds[key] = value
        return key

    if intent.date_start and intent.date_end:
        start_key = add_bind("date_start", intent.date_start)
        end_key = add_bind("date_end", intent.date_end)
        start_column = settings.get("DW_ACTIVE_START_COLUMN", "START_DATE")
        end_column = settings.get("DW_ACTIVE_END_COLUMN", "END_DATE")
        requested_col = settings.get("DW_DATE_COLUMN", "REQUEST_DATE")
        expiry_col = settings.get("DW_EXPIRY_COLUMN", "END_DATE")

        if intent.when_kind == "requested":
            where.append(f"{str(requested_col).upper()} BETWEEN :{start_key} AND :{end_key}")
        elif intent.when_kind == "expiring":
            where.append(f"{str(expiry_col).upper()} BETWEEN :{start_key} AND :{end_key}")
        else:
            where.append(
                f"( {str(start_column).upper()} <= :{end_key} AND {str(end_column).upper()} >= :{start_key} )"
            )

    equals_synonyms = _expand_request_type

    for column, values in intent.eq_filters:
        normalized_col = column.strip().upper()
        alias_targets = eq_alias.get(normalized_col, [normalized_col])
        alias_targets = [str(col).strip().upper() for col in alias_targets if str(col).strip()]
        if not alias_targets:
            alias_targets = [normalized_col]

        if normalized_col == "REQUEST_TYPE":
            eq_vals, prefixes, contains_tokens = equals_synonyms(values, settings)
        else:
            eq_vals = [val.strip().upper() for val in values if val and val.strip()]
            prefixes = []
            contains_tokens = []

        bind_names: List[str] = []
        for value in eq_vals:
            bind_names.append(add_bind("eq", value))

        predicates: List[str] = []
        if bind_names:
            for target in alias_targets:
                predicates.append(
                    "UPPER(TRIM({col})) IN ({vals})".format(
                        col=target, vals=", ".join(f":{name}" for name in bind_names)
                    )
                )

        for prefix in prefixes:
            bind = add_bind("pre", f"{prefix}%")
            for target in alias_targets:
                predicates.append(f"UPPER(TRIM({target})) LIKE :{bind}")

        for token in contains_tokens:
            bind = add_bind("contains", f"%{token}%")
            for target in alias_targets:
                predicates.append(f"UPPER(TRIM({target})) LIKE :{bind}")

        if predicates:
            where.append("(" + " OR ".join(predicates) + ")")

    for column, values in intent.neq_filters:
        normalized_col = column.strip().upper()
        bind_names = [add_bind("neq", value.strip().upper()) for value in values if value and value.strip()]
        if bind_names:
            predicates = [f"UPPER(TRIM({normalized_col})) <> :{name}" for name in bind_names]
            where.append("(" + " AND ".join(predicates) + ")")

    def _build_like_predicate(column_name: str, values_list: List[str], negate: bool = False) -> None:
        if not values_list:
            return
        bind_names = [add_bind("nlike" if negate else "like", f"%{value.strip().upper()}%") for value in values_list if value and value.strip()]
        if not bind_names:
            return
        expr = " AND ".join(
            f"UPPER(NVL({column_name.strip().upper()},'')) {'NOT ' if negate else ''}LIKE :{name}"
            for name in bind_names
        )
        where.append(f"({expr})")

    for column, values in intent.contains:
        _build_like_predicate(column, values, negate=False)

    for column, values in intent.not_contains:
        _build_like_predicate(column, values, negate=True)

    for columns in intent.empty_any:
        clauses = [f"TRIM(NVL({col.strip().upper()},'')) = ''" for col in columns if col and col.strip()]
        if clauses:
            where.append("(" + " OR ".join(clauses) + ")")

    for columns in intent.empty_all:
        clauses = [f"TRIM(NVL({col.strip().upper()},'')) = ''" for col in columns if col and col.strip()]
        if clauses:
            where.append("(" + " AND ".join(clauses) + ")")

    for column in intent.not_empty:
        if column and column.strip():
            where.append(f"TRIM(NVL({column.strip().upper()},'')) <> ''")

    for column, operator, values in intent.numeric:
        if not values:
            continue
        normalized_col = column.strip().upper()
        if operator == "between" and len(values) == 2:
            lower = add_bind("num_a", values[0])
            upper = add_bind("num_b", values[1])
            where.append(f"NVL({normalized_col},0) BETWEEN :{lower} AND :{upper}")
        else:
            operator_map = {"gt": ">", "gte": ">=", "lt": "<", "lte": "<="}
            sql_op = operator_map.get(operator)
            if not sql_op:
                continue
            for value in values:
                bind = add_bind("num", value)
                where.append(f"NVL({normalized_col},0) {sql_op} :{bind}")

    if intent.fts_groups:
        group_clauses: List[str] = []
        for group in intent.fts_groups:
            token_clauses: List[str] = []
            for token in group:
                bind = add_bind("fts", f"%{token.strip().upper()}%")
                per_column = [f"UPPER(NVL({col},'')) LIKE :{bind}" for col in fts_cols]
                token_clauses.append("(" + " OR ".join(per_column) + ")")
            if token_clauses:
                group_clauses.append("(" + " AND ".join(token_clauses) + ")")
        if group_clauses:
            where.append("(" + " OR ".join(group_clauses) + ")")

    select_clause = settings.get("DW_SELECT_ALL_DEFAULT")
    if not isinstance(select_clause, str) or not select_clause.strip():
        select_clause = f'SELECT * FROM "{table}"'
    else:
        select_clause = select_clause.strip()

    sql = select_clause
    if where:
        sql += "\nWHERE " + "\n  AND ".join(where)

    if intent.order_by:
        order_parts = [f"{col.strip().upper()} {'ASC' if direction == 'asc' else 'DESC'}" for col, direction in intent.order_by if col]
        if order_parts:
            sql += "\nORDER BY " + ", ".join(order_parts)
    else:
        default_order = str(settings.get("DW_DATE_COLUMN", "REQUEST_DATE") or "REQUEST_DATE").upper()
        sql += f"\nORDER BY {default_order} DESC"

    if intent.offset is not None or intent.limit is not None:
        offset_value = intent.offset or 0
        limit_value = intent.limit or 100
        offset_bind = add_bind("offset", offset_value)
        limit_bind = add_bind("limit", limit_value)
        sql += f"\nOFFSET :{offset_bind} ROWS FETCH NEXT :{limit_bind} ROWS ONLY"

    return sql, binds


__all__ = ["RateIntent", "parse_time_window", "parse_rate_comment", "build_sql"]

