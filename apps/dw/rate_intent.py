from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Tuple

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
    empty_any: List[List[str]] = field(default_factory=list)
    empty_all: List[List[str]] = field(default_factory=list)
    empty: List[str] = field(default_factory=list)
    not_empty: List[str] = field(default_factory=list)
    fts_groups: List[List[str]] = field(default_factory=list)
    order_by: str = ""


_OR_SPLIT = r"\s+or\s+|\s*\|\|\s*|،\s*|\s+او\s+|\s+أو\s+"
_COLNAME = r"[A-Za-z0-9_]+"


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


def parse_structured_comment(comment: str) -> RateIntent:
    intent = RateIntent()
    segments = [seg.strip() for seg in re.split(r";|\n", comment or "") if seg.strip()]
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
            intent.eq_filters.append(eq_block)
            continue
        neq_block = _parse_eq_block("neq", raw)
        if neq_block:
            intent.neq_filters.append(neq_block)
            continue
        contains_block = _parse_eq_block("contains", raw)
        if contains_block:
            intent.contains.append(contains_block)
            continue
        not_contains_block = _parse_eq_block("not_contains", raw)
        if not_contains_block:
            intent.not_contains.append(not_contains_block)
            continue
        if lowered.startswith("empty_any:"):
            cols = [c.strip().upper() for c in raw.split(":", 1)[1].split(",") if c.strip()]
            if cols:
                intent.empty_any.append(cols)
            continue
        if lowered.startswith("empty_all:"):
            cols = [c.strip().upper() for c in raw.split(":", 1)[1].split(",") if c.strip()]
            if cols:
                intent.empty_all.append(cols)
            continue
        if lowered.startswith("empty:"):
            cols = [c.strip().upper() for c in raw.split(":", 1)[1].split(",") if c.strip()]
            intent.empty.extend(cols)
            continue
        if lowered.startswith("not_empty:"):
            cols = [c.strip().upper() for c in raw.split(":", 1)[1].split(",") if c.strip()]
            intent.not_empty.extend(cols)
            continue
        if lowered.startswith("order_by:"):
            intent.order_by = raw.split(":", 1)[1].strip()
            continue
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

    where_sql = and_join(clauses) if clauses else "(TRUE)"
    return where_sql, binds


__all__ = [
    "RateIntent",
    "build_where_and_binds",
    "parse_structured_comment",
]
