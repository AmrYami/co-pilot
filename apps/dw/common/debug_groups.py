"""Utilities for building human-friendly boolean group debug info."""
# English-only comments.
from __future__ import annotations

from string import ascii_uppercase
from typing import Any, Dict, Iterable, List, Tuple

from apps.dw.common.bool_groups import Group, infer_boolean_groups
from apps.dw.common.eq_aliases import resolve_eq_targets


def build_boolean_where(group: dict) -> Tuple[str, Dict[str, str], str]:
    """
    Build a SQL WHERE fragment for a single boolean group descriptor.

    - Within the same field we OR values via an ``IN`` list.
    - Across different fields we AND the clauses together.
    - For expanded/alias columns we OR the ``IN`` clause across each column.

    Returns ``(where_sql, binds, binds_text)`` where ``where_sql`` already
    includes parentheses around the combined expression.
    """

    bind_index = 0
    binds: Dict[str, str] = {}
    field_clauses: List[str] = []

    fields = group.get("fields") if isinstance(group, dict) else None
    if not isinstance(fields, list):
        fields = []

    for raw_field in fields:
        if not isinstance(raw_field, dict):
            continue

        raw_values = raw_field.get("values") if isinstance(raw_field.get("values"), list) else []
        cleaned_values: List[str] = []
        seen_values: set[str] = set()
        for value in raw_values:
            text = str(value or "").strip()
            if not text:
                continue
            key = text.upper()
            if key in seen_values:
                continue
            seen_values.add(key)
            cleaned_values.append(text)

        if not cleaned_values:
            continue

        columns = raw_field.get("expanded_columns") or [raw_field.get("field")]
        if not isinstance(columns, list):
            columns = [columns]
        normalized_columns = [str(col).strip() for col in columns if str(col or "").strip()]
        if not normalized_columns:
            continue

        op = str(raw_field.get("op") or "eq").lower()
        bind_names: List[str] = []
        for value in cleaned_values:
            bind_name = f"eq_bg_{bind_index}"
            bind_index += 1
            if op == "like":
                bind_value = value
                if value and "%" not in value:
                    bind_value = f"%{value}%"
                binds[bind_name] = bind_value.upper()
            else:
                binds[bind_name] = value.upper()
            bind_names.append(bind_name)

        if not bind_names:
            continue

        if op == "like":
            per_column: List[str] = []
            for column in normalized_columns:
                lhs = f"UPPER(TRIM({column}))"
                comparisons = [f"{lhs} LIKE UPPER(TRIM(:{name}))" for name in bind_names]
                per_column.append("(" + " OR ".join(comparisons) + ")")
            if per_column:
                field_clauses.append("(" + " OR ".join(per_column) + ")")
            continue

        in_list = ", ".join(f"UPPER(TRIM(:{name}))" for name in bind_names)
        per_column = [
            f"UPPER(TRIM({column})) IN ({in_list})" for column in normalized_columns
        ]
        if per_column:
            joined = " OR ".join(per_column)
            field_clauses.append(f"({joined})")

    if not field_clauses:
        return "", {}, ""

    where_sql = " AND ".join(field_clauses)
    if not where_sql.startswith("("):
        where_sql = f"({where_sql})"
    binds_text = ", ".join(f"{key}='{str(value).upper()}'" for key, value in binds.items())
    return where_sql, binds, binds_text
from apps.dw.settings import get_settings


def _human_join(items: Iterable[str], conj: str = " OR ") -> str:
    parts = [str(item).strip() for item in items if str(item).strip()]
    if not parts:
        return ""
    if len(parts) == 1:
        return parts[0]
    return conj.join(parts)


def _pretty_field(field: str, op: str, values: List[str]) -> str:
    vals = _human_join(values)
    if not vals:
        return field
    if op == "like":
        return f"{field} CONTAINS ({vals})"
    return f"{field} = ({vals})"


def _coerce_columns(columns: Any) -> List[str]:
    if isinstance(columns, list):
        return [str(col) for col in columns if str(col).strip()]
    if isinstance(columns, (set, tuple)):
        return [str(col) for col in columns if str(col).strip()]
    if isinstance(columns, dict):
        coerced: List[str] = []
        for value in columns.values():
            coerced.extend(_coerce_columns(value))
        return coerced
    if isinstance(columns, str):
        return [part.strip() for part in columns.split(",") if part.strip()]
    return []


def _fallback_fts_columns() -> List[str]:
    settings = get_settings() or {}
    columns_setting = settings.get("DW_FTS_COLUMNS") if isinstance(settings, dict) else None
    columns: List[str] = []
    if isinstance(columns_setting, dict):
        for key in ("Contract", "*"):
            if key in columns_setting:
                columns = _coerce_columns(columns_setting.get(key))
                if columns:
                    break
    elif columns_setting:
        columns = _coerce_columns(columns_setting)
    return columns


def _coerce_question(question: str | None) -> str:
    if question is None:
        return ""
    return str(question).strip()


_FIELD_PRIORITY = {
    "ENTITY": 0,
    "REPRESENTATIVE_EMAIL": 1,
    "REPRESENTATIVE": 1,
    "STAKEHOLDER": 2,
    "STAKEHOLDERS": 2,
    "DEPARTMENT": 3,
    "DEPARTMENTS": 3,
}


def _field_sort_key(name: str, index: int) -> Tuple[int, int]:
    normalized = (name or "").strip().upper()
    rank = _FIELD_PRIORITY.get(normalized, 100 + index)
    return rank, index


def _dedupe_values(values: Iterable[str]) -> List[str]:
    seen: set[str] = set()
    out: List[str] = []
    for value in values:
        text = str(value or "").strip()
        if not text:
            continue
        key = text.upper()
        if key in seen:
            continue
        seen.add(key)
        out.append(text)
    return out


def _wrap_column(expr: str, *, ci: bool, trim: bool) -> str:
    wrapped = expr
    if trim:
        wrapped = f"TRIM({wrapped})"
    if ci:
        wrapped = f"UPPER({wrapped})"
    return wrapped


def _wrap_bind(name: str, *, ci: bool, trim: bool) -> str:
    expr = f":{name}"
    if trim:
        expr = f"TRIM({expr})"
    if ci:
        expr = f"UPPER({expr})"
    return expr


def _build_boolean_where(
    groups: List[Group],
    *,
    ci: bool,
    trim: bool,
) -> Tuple[str, Dict[str, Any]]:
    bind_index = 0
    binds: Dict[str, Any] = {}
    clauses: List[str] = []

    for group in groups:
        field_entries: List[Tuple[str, Dict[str, Any]]] = []
        for idx, (column, values, op) in enumerate(group.field_terms):
            if not column:
                continue
            cleaned_values = _dedupe_values(values)
            if not cleaned_values:
                continue
            expanded = resolve_eq_targets(column) or [column]
            entry = {
                "field": column,
                "op": "like" if op == "like" else "eq",
                "values": cleaned_values,
                "expanded_columns": expanded,
            }
            field_entries.append((f"{column}", entry))

        if not field_entries:
            continue

        # Stable ordering per group
        ordered_fields = [entry for _, entry in sorted(
            (
                (_field_sort_key(name, idx), data)
                for idx, (name, data) in enumerate(field_entries)
            ),
            key=lambda item: item[0],
        )]

        field_clauses: List[str] = []
        for entry in ordered_fields:
            field_name = (entry.get("field") or "").strip()
            op = entry.get("op", "eq")
            values_list = entry.get("values") or []
            expanded_cols = entry.get("expanded_columns") or [field_name]
            if not values_list:
                continue

            bind_names: List[str] = []
            for value in values_list:
                bind_name = f"eq_bg_{bind_index}"
                bind_index += 1
                if op == "like":
                    binds[bind_name] = f"%{value}%"
                else:
                    binds[bind_name] = value.upper()
                bind_names.append(bind_name)
            if not bind_names:
                continue

            if op == "like":
                column_clauses: List[str] = []
                for col in expanded_cols:
                    column_expr = _wrap_column(col, ci=ci, trim=trim)
                    comparisons = [
                        f"{column_expr} LIKE {_wrap_bind(name, ci=ci, trim=trim)}"
                        for name in bind_names
                    ]
                    if comparisons:
                        column_clauses.append("(" + " OR ".join(comparisons) + ")")
                if column_clauses:
                    field_clauses.append("(" + " OR ".join(column_clauses) + ")")
                continue

            bind_expr = ", ".join(
                _wrap_bind(name, ci=ci, trim=trim) for name in bind_names
            )
            column_checks: List[str] = []
            for col in expanded_cols:
                column_expr = _wrap_column(col, ci=ci, trim=trim)
                column_checks.append(f"{column_expr} IN ({bind_expr})")
            if column_checks:
                if len(column_checks) == 1:
                    field_clauses.append(column_checks[0])
                else:
                    field_clauses.append("(" + " OR ".join(column_checks) + ")")

        if field_clauses:
            clauses.append("(" + " AND ".join(field_clauses) + ")")

    if not clauses:
        return "", {}

    where_text = " OR ".join(clauses) if len(clauses) > 1 else clauses[0]
    return where_text, binds


def build_boolean_debug(
    question: str,
    fts_columns: List[str] | None = None,
    *,
    case_insensitive: bool = True,
    trim_values: bool = True,
) -> Dict[str, Any]:
    """Return debug metadata for inferred boolean groups."""

    groups: List[Group] = infer_boolean_groups(_coerce_question(question))
    effective_columns = list(fts_columns or [])
    if not effective_columns:
        effective_columns = _fallback_fts_columns()

    blocks: List[Dict[str, Any]] = []
    lines_for_summary: List[str] = []

    for index, group in enumerate(groups):
        block_id = ascii_uppercase[index] if index < len(ascii_uppercase) else f"#{index + 1}"
        fts_tokens = list(group.fts_tokens)
        fts_text = f"FTS({' OR '.join(fts_tokens)})" if fts_tokens else ""

        raw_fields: List[Tuple[str, Dict[str, Any]]] = []
        for field_index, (column, values, op) in enumerate(group.field_terms):
            if not column:
                continue
            expanded = resolve_eq_targets(column) or [column]
            entry = {
                "field": column,
                "op": "like" if op == "like" else "eq",
                "values": list(values),
                "expanded_columns": expanded,
            }
            raw_fields.append((column, entry))

        ordered_entries = [
            entry
            for _, entry in sorted(
                (
                    (_field_sort_key(name, idx), data)
                    for idx, (name, data) in enumerate(raw_fields)
                ),
                key=lambda item: item[0],
            )
        ]

        field_parts: List[str] = []
        for entry in ordered_entries:
            field_parts.append(
                _pretty_field(entry.get("field"), entry.get("op", "eq"), entry.get("values") or [])
            )

        pretty_bits = []
        if fts_text:
            pretty_bits.append(fts_text)
        pretty_bits.extend(field_parts)
        block_text = " AND ".join(bit for bit in pretty_bits if bit) or "TRUE"
        lines_for_summary.append(f"({block_text})")

        blocks.append(
            {
                "id": block_id,
                "fts": fts_tokens,
                "fts_columns_count": len(effective_columns),
                "fields": ordered_entries,
            }
        )

    summary = " OR ".join(lines_for_summary) if lines_for_summary else "(TRUE)"

    where_text, bind_values = _build_boolean_where(
        groups,
        ci=case_insensitive,
        trim=trim_values,
    )

    binds_text = ""
    if bind_values:
        parts = []
        for key, value in bind_values.items():
            if isinstance(value, str):
                preview = value.upper() if case_insensitive else value
            else:
                preview = value
            parts.append(f"{key}={preview}")
        binds_text = ", ".join(parts)

    result: Dict[str, Any] = {"summary": summary, "blocks": blocks}
    if where_text:
        result["where_text"] = where_text
    if bind_values:
        result["binds"] = bind_values
    if binds_text:
        result["binds_text"] = binds_text

    # Best-effort plan metadata for parity with legacy builder
    try:
        from apps.dw.contracts.builder import build_boolean_where_from_plan

        if groups:
            settings_obj = None
            try:
                settings_obj = get_settings()
            except Exception:  # pragma: no cover - defensive fallback
                settings_obj = None
            plan_candidate = build_boolean_where_from_plan(
                groups,
                settings_obj,
                fts_columns=effective_columns,
            )
            if isinstance(plan_candidate, dict) and plan_candidate.get("where_sql"):
                plan_result = {
                    "where_sql": plan_candidate.get("where_sql", ""),
                    "where_text": plan_candidate.get("where_text", ""),
                    "binds": dict(plan_candidate.get("binds") or {}),
                }
                if plan_candidate.get("binds_text"):
                    plan_result["binds_text"] = plan_candidate["binds_text"]
                result["plan"] = plan_result
    except Exception:  # pragma: no cover - ignore plan issues in debug mode
        pass

    return result
