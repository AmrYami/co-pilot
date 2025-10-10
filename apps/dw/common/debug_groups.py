"""Utilities for building human-friendly boolean group debug info."""
# English-only comments.
from __future__ import annotations

from string import ascii_uppercase
from typing import Any, Dict, Iterable, List

from apps.dw.common.bool_groups import Group, infer_boolean_groups
from apps.dw.common.eq_aliases import resolve_eq_targets
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


def build_boolean_debug(question: str, fts_columns: List[str] | None = None) -> Dict[str, Any]:
    """Return debug metadata for inferred boolean groups."""

    groups: List[Group] = infer_boolean_groups(_coerce_question(question))
    effective_columns = list(fts_columns or [])
    if not effective_columns:
        effective_columns = _fallback_fts_columns()

    blocks: List[Dict[str, Any]] = []
    lines_for_summary: List[str] = []
    allowed_for_debug: set[str] = set()
    for index, group in enumerate(groups):
        block_id = ascii_uppercase[index] if index < len(ascii_uppercase) else f"#{index + 1}"
        fts_tokens = list(group.fts_tokens)
        fts_text = f"FTS({' OR '.join(fts_tokens)})" if fts_tokens else ""

        field_entries: List[Dict[str, Any]] = []
        field_parts: List[str] = []
        for column, values, op in group.field_terms:
            expanded = resolve_eq_targets(column)  # يعتمد على DB settings / fallbacks
            if expanded:
                allowed_for_debug.update(col.strip().upper() for col in expanded if col)
            if column:
                allowed_for_debug.add(column.strip().upper())
            entry = {
                "field": column,
                "op": "eq" if op == "eq" else "like",
                "values": list(values),
                "expanded_columns": expanded,
            }
            field_entries.append(entry)
            field_parts.append(_pretty_field(column, op, values))

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
                "fields": field_entries,
            }
        )

    summary = " OR ".join(lines_for_summary) if lines_for_summary else "(TRUE)"

    where_text = ""
    binds_preview: Dict[str, Any] = {}
    try:
        from apps.dw.contracts.builder import build_boolean_where_from_question

        if groups:
            allowed = {col for col in allowed_for_debug if col}
            where_text, binds_preview = build_boolean_where_from_question(
                _coerce_question(question),
                fts_columns=effective_columns,
                allowed_columns=allowed or set(),
            )
    except Exception:  # pragma: no cover - debug fallback when contracts.builder unavailable
        where_text = ""
        binds_preview = {}

    result: Dict[str, Any] = {"summary": summary, "blocks": blocks}
    if where_text:
        result["where_text"] = where_text
    if binds_preview:
        ordered = sorted(binds_preview.items())
        result["binds_text"] = ", ".join(f"{name}={value}" for name, value in ordered)
    return result
