from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Tuple, Optional
import difflib

from apps.dw.sql_shared import (
    and_join,
    eq_alias_columns,
    explicit_columns,
    fts_columns_for,
    in_list_sql,
    is_empty_sql,
    like_sql,
    not_empty_sql,
    or_join,
    request_type_synonyms,
)


@dataclass
class RateIntent:
    eq_filters: List[Tuple[str, List[str]]] = field(default_factory=list)
    neq_filters: List[Tuple[str, List[str]]] = field(default_factory=list)
    contains: List[Tuple[str, List[str]]] = field(default_factory=list)
    not_contains: List[Tuple[str, List[str]]] = field(default_factory=list)
    numeric: List[Tuple[str, str, List[str]]] = field(default_factory=list)
    empty_any: List[List[str]] = field(default_factory=list)
    empty_all: List[List[str]] = field(default_factory=list)
    empty: List[str] = field(default_factory=list)
    not_empty: List[str] = field(default_factory=list)
    fts_groups: List[List[str]] = field(default_factory=list)
    group_by: List[str] = field(default_factory=list)
    aggregations: List[Dict[str, Any]] = field(default_factory=list)
    gross: Optional[bool] = None
    order_by: str = ""


_OR_SPLIT = r"\s+or\s+|\s*\|\|\s*|،\s*|\s+او\s+|\s+أو\s+"
_COLNAME = r"[A-Za-z0-9_]+"


_AGG_FUNC = r"[A-Za-z0-9_]+"


def _split_agg_parts(value: str) -> List[str]:
    parts: List[str] = []
    current: List[str] = []
    depth = 0
    for ch in value:
        if ch == "," and depth == 0:
            part = "".join(current).strip()
            if part:
                parts.append(part)
            current = []
            continue
        current.append(ch)
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth = max(0, depth - 1)
    tail = "".join(current).strip()
    if tail:
        parts.append(tail)
    return parts


def _parse_agg_expr(raw: str, resolver) -> Dict[str, Any] | None:
    text = (raw or "").strip().rstrip(";")
    if not text:
        return None
    match = re.match(
        rf"^(?P<func>{_AGG_FUNC})\s*\(\s*(?P<body>.*?)\s*\)\s*(?:as\s+(?P<alias>[A-Za-z0-9_\"']+))?$",
        text,
        flags=re.IGNORECASE,
    )
    if not match:
        return None
    func = match.group("func").strip().upper()
    body = (match.group("body") or "").strip()
    distinct = False
    if body.lower().startswith("distinct "):
        distinct = True
        body = body[8:].strip()
    column = body or "*"
    alias_raw = match.group("alias")
    alias = None
    if alias_raw:
        alias = alias_raw.strip().strip('"').strip("'")
        alias = alias.upper()
    resolved_col = column
    if column != "*":
        try:
            resolved_col = resolver(column)
        except ValueError:
            resolved_col = column.strip().upper()
    return {
        "func": func,
        "column": resolved_col if resolved_col else column.upper(),
        "distinct": distinct,
        "alias": alias,
    }


def _split_values(value: str) -> List[str]:
    parts = re.split(_OR_SPLIT, value, flags=re.IGNORECASE)
    return [p.strip() for p in parts if p and p.strip()]


def _parse_eq_block(prefix: str, line: str) -> Tuple[str, List[str]] | None:
    pattern = rf"^{prefix}\s*:\s*({_COLNAME})\s*=\s*(.+)$"
    match = re.match(pattern, line.strip(), flags=re.IGNORECASE)
    if not match:
        return None
    column = match.group(1).strip().upper()
    values = [val.strip() for val in _split_values(match.group(2)) if val.strip()]
    return column, values


_NUM_PATTERN = re.compile(
    rf"^\s*({_COLNAME})\s*(>=|<=|<>|!=|=|>|<|between)\s*(.+)$",
    flags=re.IGNORECASE,
)


def _parse_numeric_block(line: str) -> Tuple[str, str, List[str]] | None:
    """Parse `num:` hints like `num: VAT > 200` or `num: VAT between 100 and 200`."""
    body = line.split(":", 1)[1].strip() if ":" in line else line
    match = _NUM_PATTERN.match(body)
    if not match:
        return None
    column = match.group(1).strip().upper()
    op = match.group(2).strip().lower()
    rhs = (match.group(3) or "").strip()
    if not column or not rhs:
        return None
    if op == "between":
        parts = re.split(r"(?i)\band\b", rhs)
        values = [p.strip() for p in parts if p and p.strip()]
        if len(values) != 2:
            return None
        return column, op, values
    values = [rhs]
    return column, op, values


def parse_structured_comment(
    comment: str,
    *,
    alias_map: Optional[Dict[str, Iterable[str]]] = None,
    allowed_columns: Optional[Iterable[str]] = None,
) -> RateIntent:
    intent = RateIntent()
    segments = [seg.strip() for seg in re.split(r";|\n", comment or "") if seg.strip()]

    alias_map_upper: Dict[str, List[str]] = {}
    if isinstance(alias_map, dict):
        for key, cols in alias_map.items():
            targets = [str(c or "").strip().upper() for c in (cols or []) if str(c or "").strip()]
            if targets:
                alias_map_upper[str(key or "").strip().upper()] = targets

    allowed_set = {str(c or "").strip().upper() for c in (allowed_columns or []) if str(c or "").strip()}

    def _resolve_column(token: str) -> str:
        col = str(token or "").strip().upper()
        if not col:
            return col
        if col in allowed_set or col in alias_map_upper:
            return col
        for targets in alias_map_upper.values():
            if col in targets:
                return col
        candidates = list(allowed_set) + list(alias_map_upper.keys())
        suggestion = difflib.get_close_matches(col, candidates, n=1)
        hint = f" Did you mean {suggestion[0]}?" if suggestion else ""
        raise ValueError(f"Unrecognised column '{token}'.{hint}")

    for raw in segments:
        lowered = raw.lower()
        if lowered.startswith("fts:"):
            values = raw.split(":", 1)[1].strip()
            for token in _split_values(values):
                if token:
                    intent.fts_groups.append([token.strip()])
            continue
        eq_block = _parse_eq_block("eq", raw)
        if eq_block:
            col, vals = eq_block
            intent.eq_filters.append((_resolve_column(col), vals))
            continue
        neq_block = _parse_eq_block("neq", raw)
        if neq_block:
            col, vals = neq_block
            intent.neq_filters.append((_resolve_column(col), vals))
            continue
        contains_block = _parse_eq_block("contains", raw)
        if contains_block:
            col, vals = contains_block
            intent.contains.append((_resolve_column(col), vals))
            continue
        not_contains_block = _parse_eq_block("not_contains", raw)
        if not_contains_block:
            col, vals = not_contains_block
            intent.not_contains.append((_resolve_column(col), vals))
            continue
        if lowered.startswith("num:"):
            numeric_block = _parse_numeric_block(raw)
            if numeric_block:
                col, op, values = numeric_block
                intent.numeric.append((_resolve_column(col), op, values))
            continue
        if lowered.startswith("empty_any:"):
            cols = [ _resolve_column(c) for c in raw.split(":", 1)[1].split(",") if c.strip() ]
            if cols:
                intent.empty_any.append(cols)
            continue
        if lowered.startswith("empty_all:"):
            cols = [ _resolve_column(c) for c in raw.split(":", 1)[1].split(",") if c.strip() ]
            if cols:
                intent.empty_all.append(cols)
            continue
        if lowered.startswith("empty:"):
            cols = [ _resolve_column(c) for c in raw.split(":", 1)[1].split(",") if c.strip() ]
            intent.empty.extend(cols)
            continue
        if lowered.startswith("not_empty:"):
            cols = [ _resolve_column(c) for c in raw.split(":", 1)[1].split(",") if c.strip() ]
            intent.not_empty.extend(cols)
            continue
        if lowered.startswith("order_by:"):
            body = raw.split(":", 1)[1].strip()
            tokens = [tok for tok in body.replace(";", " ").split() if tok]
            if tokens:
                col_token = tokens[0]
                direction_token = tokens[1] if len(tokens) > 1 else "DESC"
                try:
                    col_norm = _resolve_column(col_token)
                except ValueError:
                    col_norm = col_token.strip().upper()
                direction = direction_token.strip().upper()
                if direction not in {"ASC", "DESC"}:
                    direction = "DESC"
                intent.order_by = f"{col_norm} {direction}"
            continue
        if lowered.startswith("group_by:"):
            cols = raw.split(":", 1)[1]
            values = [frag.strip() for frag in cols.replace(";", "").split(",") if frag.strip()]
            normalized = []
            for value in values:
                try:
                    normalized.append(_resolve_column(value))
                except ValueError:
                    normalized.append(value.strip().upper())
            if normalized:
                intent.group_by.extend(normalized)
            continue
        if lowered.startswith("gross:"):
            flag = raw.split(":", 1)[1].strip().lower()
            if flag in {"true", "1", "yes"}:
                intent.gross = True
            elif flag in {"false", "0", "no"}:
                intent.gross = False
            continue
        if lowered.startswith("agg:"):
            body = raw.split(":", 1)[1].strip()
            for segment in _split_agg_parts(body):
                parsed = _parse_agg_expr(segment, _resolve_column)
                if parsed:
                    intent.aggregations.append(parsed)
            continue
    if intent.group_by:
        seen_cols: set[str] = set()
        deduped: List[str] = []
        for col in intent.group_by:
            token = str(col or "").strip().upper()
            if token and token not in seen_cols:
                seen_cols.add(token)
                deduped.append(token)
        intent.group_by = deduped
    if intent.aggregations:
        normalized_aggs: List[Dict[str, Any]] = []
        seen_agg: set[tuple[str, str, bool, str]] = set()
        for agg in intent.aggregations:
            func = str(agg.get("func") or "").upper()
            column = str(agg.get("column") or "").upper() if agg.get("column") != "*" else "*"
            distinct = bool(agg.get("distinct"))
            alias = str(agg.get("alias") or "").upper()
            key = (func, column, distinct, alias)
            if not func or (column == "" and column != "*"):
                continue
            if key in seen_agg:
                continue
            seen_agg.add(key)
            normalized_aggs.append(
                {
                    "func": func,
                    "column": column if column else "",
                    "distinct": distinct,
                    "alias": alias or None,
                }
            )
        intent.aggregations = normalized_aggs
    return intent


def _normalized(values: Iterable[str]) -> List[str]:
    cleaned: List[str] = []
    seen: set[str] = set()
    for raw in values:
        if not isinstance(raw, str):
            continue
        text = raw.strip()
        if not text:
            continue
        upper = text.upper()
        if upper in seen:
            continue
        seen.add(upper)
        cleaned.append(upper)
    return cleaned


def _expand_request_type_equals(values: List[str]) -> Tuple[List[str], List[str], List[str]]:
    mapping = request_type_synonyms().get("Contract.REQUEST_TYPE") or {}
    equals: List[str] = []
    prefixes: List[str] = []
    contains_tokens: List[str] = []
    for raw in values:
        normalized = (raw or "").strip()
        if not normalized:
            continue
        equals.append(normalized)
        for key, rules in mapping.items():
            if not isinstance(rules, dict):
                continue
            key_text = str(key).strip()
            if not key_text:
                continue
            all_equals = _normalized(rules.get("equals", [])) or [key_text.upper()]
            if normalized.upper() == key_text.upper() or normalized.upper() in all_equals:
                equals.extend(all_equals)
                prefixes.extend(_normalized(rules.get("prefix", [])))
                contains_tokens.extend(_normalized(rules.get("contains", [])))
                break
    return _normalized(equals), _normalized(prefixes), _normalized(contains_tokens)


def build_where_and_binds(table: str, intent: RateIntent) -> Tuple[str, Dict[str, Any]]:
    binds: Dict[str, Any] = {}
    clauses: List[str] = []
    alias_groups = eq_alias_columns()

    for column, raw_values in intent.eq_filters:
        col = column.upper()
        values = _normalized(raw_values)
        if not values:
            continue
        if col == "REQUEST_TYPE":
            equals_vals, prefixes, contains_tokens = _expand_request_type_equals(values)
            bind_names: List[str] = []
            for value in equals_vals:
                name = f"eq_{len(binds)}"
                binds[name] = value.upper()
                bind_names.append(name)
            eq_clause = in_list_sql(col, bind_names, upper_trim=True) if bind_names else ""
            synonym_parts: List[str] = []
            for prefix in prefixes:
                name = f"pre_{len(binds)}"
                binds[name] = f"{prefix.upper()}%"
                synonym_parts.append(like_sql(col, name, negate=False, nvl=True, upper=True))
            for token in contains_tokens:
                name = f"contains_{len(binds)}"
                binds[name] = f"%{token.upper()}%"
                synonym_parts.append(like_sql(col, name, negate=False, nvl=True, upper=True))
            clause_parts = [eq_clause] if eq_clause else []
            clause_parts.extend(synonym_parts)
            if clause_parts:
                clauses.append(or_join(clause_parts))
            continue

        alias_targets = alias_groups.get(col)
        bind_names = []
        for value in values:
            name = f"eq_{len(binds)}"
            binds[name] = value.upper()
            bind_names.append(name)
        if alias_targets:
            per_alias = [
                in_list_sql(alias_col, bind_names, upper_trim=True)
                for alias_col in alias_targets
            ]
            clauses.append(or_join(per_alias))
        else:
            clauses.append(in_list_sql(col, bind_names, upper_trim=True))

    for column, raw_values in intent.neq_filters:
        col = column.upper()
        values = _normalized(raw_values)
        if not values:
            continue
        parts: List[str] = []
        for value in values:
            name = f"neq_{len(binds)}"
            binds[name] = value.upper()
            parts.append(f"UPPER(TRIM({col})) <> UPPER(:{name})")
        clauses.append(and_join(parts))

    for column, raw_values in intent.contains:
        col = column.upper()
        values = _normalized(raw_values)
        if not values:
            continue
        parts: List[str] = []
        for value in values:
            name = f"like_{len(binds)}"
            binds[name] = f"%{value.upper()}%"
            parts.append(like_sql(col, name, negate=False, nvl=True, upper=True))
        clauses.append(and_join(parts))

    for column, raw_values in intent.not_contains:
        col = column.upper()
        values = _normalized(raw_values)
        if not values:
            continue
        parts: List[str] = []
        for value in values:
            name = f"nlike_{len(binds)}"
            binds[name] = f"%{value.upper()}%"
            parts.append(like_sql(col, name, negate=True, nvl=True, upper=True))
        clauses.append(and_join(parts))

    for col in intent.empty:
        clauses.append(is_empty_sql(col.upper()))

    for col in intent.not_empty:
        clauses.append(not_empty_sql(col.upper()))

    for group in intent.empty_any:
        clauses.append(or_join(is_empty_sql(col.upper()) for col in group))

    for group in intent.empty_all:
        clauses.append(and_join(is_empty_sql(col.upper()) for col in group))

    if intent.fts_groups:
        fts_cols = fts_columns_for(table) or explicit_columns()
        if fts_cols:
            group_clauses: List[str] = []
            for tokens in intent.fts_groups:
                token_clauses: List[str] = []
                for token in tokens:
                    name = f"fts_{len(binds)}"
                    binds[name] = f"%{token.strip().upper()}%"
                    per_column = [
                        like_sql(column, name, negate=False, nvl=True, upper=True)
                        for column in fts_cols
                    ]
                    token_clauses.append(or_join(per_column))
                if token_clauses:
                    group_clauses.append(and_join(token_clauses))
            if group_clauses:
                clauses.append(or_join(group_clauses))

    for column, op, raw_values in intent.numeric:
        col = column.upper()
        values = [v for v in raw_values if isinstance(v, str) and v.strip()]
        if not values:
            continue
        normalized_op = op.lower()
        if normalized_op == "between":
            if len(values) != 2:
                continue
            left_val = _coerce_numeric(values[0])
            right_val = _coerce_numeric(values[1])
            if left_val is None or right_val is None:
                continue
            left_name = f"num_{len(binds)}"
            binds[left_name] = left_val
            right_name = f"num_{len(binds)}"
            binds[right_name] = right_val
            clauses.append(f"{col} BETWEEN :{left_name} AND :{right_name}")
            continue
        comparator = {
            ">": ">",
            "<": "<",
            ">=": ">=",
            "<=": "<=",
            "=": "=",
            "==": "=",
            "!=": "<>",
            "<>": "<>",
        }.get(normalized_op)
        value = _coerce_numeric(values[0])
        if not comparator or value is None:
            continue
        name = f"num_{len(binds)}"
        binds[name] = value
        clauses.append(f"{col} {comparator} :{name}")

    where_sql = and_join(clauses) if clauses else "(TRUE)"
    return where_sql, binds


def _coerce_numeric(value: str) -> Any | None:
    text = (value or "").strip().replace(",", "")
    if not text:
        return None
    try:
        if "." in text:
            return float(text)
        return int(text)
    except ValueError:
        try:
            return float(text)
        except ValueError:
            return None


__all__ = [
    "RateIntent",
    "build_where_and_binds",
    "parse_structured_comment",
]
