"""Lightweight SQL builder used by DW routes."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

GROSS_EXPR = (
    "NVL(CONTRACT_VALUE_NET_OF_VAT,0) + CASE WHEN NVL(VAT,0) BETWEEN 0 AND 1 "
    "THEN NVL(CONTRACT_VALUE_NET_OF_VAT,0) * NVL(VAT,0) ELSE NVL(VAT,0) END"
)


def _quote_table(name: str) -> str:
    text = (name or "").strip()
    if not text:
        return '"Contract"'
    if text.startswith('"') and text.endswith('"'):
        return text
    if "." in text:
        parts = [p for p in text.split(".") if p]
        if not parts:
            return '"Contract"'
        return ".".join(_quote_table(part) for part in parts)
    return f'"{text.strip("\"")}"'


def _normalize_identifier(name: str) -> str:
    text = (name or "").strip()
    if not text:
        return ""
    if text.startswith('"') and text.endswith('"'):
        return text
    return text.upper().replace(" ", "_")


def _in_list_bind_keys(
    prefix: str,
    start_idx: int,
    values: Iterable[Any],
) -> Tuple[List[str], Dict[str, Any], int]:
    keys: List[str] = []
    binds: Dict[str, Any] = {}
    idx = start_idx
    for value in values:
        key = f"{prefix}_{idx}"
        binds[key] = value
        keys.append(f":{key}")
        idx += 1
    return keys, binds, idx


def build_eq_boolean_groups_where(
    boolean_groups: Iterable[Dict[str, Any]],
    *,
    bind_prefix: str = "eq_bg",
    start_index: int = 0,
) -> Tuple[str, Dict[str, Any], int]:
    """Render boolean ``IN`` clauses from structured boolean group filters."""

    binds: Dict[str, Any] = {}
    clauses: List[str] = []
    next_index = start_index

    for group in boolean_groups or []:
        if not isinstance(group, dict):
            continue
        fields = group.get("fields")
        if not isinstance(fields, list):
            continue
        field_clauses: List[str] = []
        for field in fields:
            if not isinstance(field, dict):
                continue
            values = [v for v in (field.get("values") or []) if v is not None]
            if not values:
                continue
            columns = field.get("expanded_columns") or [field.get("field")]
            normalized_columns = [
                _normalize_identifier(str(col)) for col in columns if col
            ]
            normalized_columns = [col for col in normalized_columns if col]
            if not normalized_columns:
                continue
            placeholders, local_binds, next_index = _in_list_bind_keys(
                bind_prefix, next_index, values
            )
            if not placeholders:
                continue
            binds.update(local_binds)
            in_list = ", ".join(placeholders)
            column_checks = [
                f"UPPER(TRIM({column})) IN ({in_list})" for column in normalized_columns
            ]
            field_clauses.append("(" + " OR ".join(column_checks) + ")")
        if field_clauses:
            clauses.append("(" + " AND ".join(field_clauses) + ")")

    if not clauses:
        return "", {}, start_index

    return "(" + " OR ".join(clauses) + ")", binds, next_index


def normalize_order_by(
    order_by: Optional[str],
    sort_by: Optional[str],
    sort_desc: Optional[Any],
) -> str:
    """Normalize order-by inputs ensuring a single direction suffix."""

    candidate = (sort_by or order_by or "REQUEST_DATE") or "REQUEST_DATE"
    text = str(candidate).strip()
    descending: Optional[bool]

    if isinstance(sort_desc, str):
        normalized = sort_desc.strip().lower()
        if normalized in {"true", "1", "yes"}:
            descending = True
        elif normalized in {"false", "0", "no"}:
            descending = False
        else:
            descending = None
    else:
        descending = bool(sort_desc) if sort_desc is not None else None

    upper = text.upper()
    if upper.endswith(" DESC"):
        text = text[: -4]
        descending = True if descending is None else descending
    elif upper.endswith(" ASC"):
        text = text[: -3]
        descending = False if descending is None else descending

    if text.upper().endswith("_DESC"):
        text = text[: -5]
        descending = True if descending is None else descending

    column = _normalize_identifier(text) or "REQUEST_DATE"
    direction = "DESC" if descending or descending is None else "ASC"
    return f"{column} {direction}"


def _apply_flags(expr: str, *, ci: bool, trim: bool) -> str:
    column_sql = expr
    if trim:
        column_sql = f"TRIM({column_sql})"
    if ci:
        column_sql = f"UPPER({column_sql})"
    return column_sql


@dataclass
class _OrderConfig:
    column: Optional[str]
    descending: bool


class QueryBuilder:
    """Composable SELECT builder with FTS, boolean groups, and EQ helpers."""

    def __init__(self, *, table: str, date_col: Optional[str] = None) -> None:
        self.table = table
        self.date_col = _normalize_identifier(date_col or "REQUEST_DATE")
        self._order = _OrderConfig(column=self.date_col, descending=True)
        self._binds: Dict[str, Any] = {}
        self._bind_idx = 0
        self._where_parts: List[str] = []
        self._notes: List[str] = []
        self._fts_engine: Any = None
        self._fts_engine_name: Optional[str] = None
        self._fts_columns: Sequence[str] = []
        self._fts_groups: List[List[str]] = []
        self._fts_operator: str = "OR"
        self._fts_min_len: int = 1
        self._group_by: List[str] = []
        self._gross: bool = False
        self._wants_all_columns: bool = True
        self._limit: Optional[int] = None

    # ------------------------------------------------------------------
    # Configuration helpers
    # ------------------------------------------------------------------
    def use_fts(
        self,
        *,
        engine: Any,
        columns: Optional[Sequence[str]],
        min_token_len: int = 2,
    ) -> "QueryBuilder":
        self._fts_engine = engine
        self._fts_engine_name = getattr(engine, "name", None)
        self._fts_columns = [
            col for col in (columns or []) if isinstance(col, str) and col.strip()
        ]
        try:
            self._fts_min_len = max(1, int(min_token_len))
        except Exception:
            self._fts_min_len = 1
        if self._fts_columns:
            self._notes.append(
                f"fts_engine={self._fts_engine_name or 'unknown'} cols={len(self._fts_columns)}"
            )
        return self

    def add_fts_group(self, group: Sequence[str], *, op: str = "OR") -> "QueryBuilder":
        if not self._fts_engine or not self._fts_columns:
            return self
        cleaned: List[str] = []
        for token in group:
            if not isinstance(token, str):
                continue
            text = token.strip()
            if len(text) < self._fts_min_len:
                continue
            cleaned.append(text)
        if not cleaned:
            return self
        self._fts_groups.append(cleaned)
        if op:
            self._fts_operator = op.upper()
        return self

    def apply_eq_filters(self, eq_filters: Iterable[Dict[str, Any]]) -> "QueryBuilder":
        buckets: Dict[tuple, List[Any]] = {}
        for filt in eq_filters or []:
            if not isinstance(filt, dict):
                continue
            column = _normalize_identifier(str(filt.get("col") or filt.get("column") or ""))
            if not column:
                continue
            ci_flag = filt.get("ci")
            trim_flag = filt.get("trim")
            ci = True if ci_flag is None else bool(ci_flag)
            tr = True if trim_flag is None else bool(trim_flag)
            synonyms = filt.get("synonyms") if isinstance(filt.get("synonyms"), dict) else None
            if synonyms:
                equals_vals = [v for v in synonyms.get("equals", []) if v is not None]
                prefix_vals = [v for v in synonyms.get("prefix", []) if v is not None]
                contains_vals = [v for v in synonyms.get("contains", []) if v is not None]

                def _dedupe_upper(items: Iterable[Any]) -> List[str]:
                    seen: set[str] = set()
                    ordered: List[str] = []
                    for raw in items:
                        if not isinstance(raw, str):
                            continue
                        text = raw.strip()
                        if not text:
                            continue
                        key = text.upper()
                        if key in seen:
                            continue
                        seen.add(key)
                        ordered.append(key)
                    return ordered

                eq_items = _dedupe_upper(equals_vals)
                prefix_items = _dedupe_upper(prefix_vals)
                contains_items = _dedupe_upper(contains_vals)

                clauses: List[str] = []
                # Synonym clauses always enforce CI + TRIM semantics to match /dw/answer.
                col_expr = _apply_flags(column, ci=True, trim=True)

                for value in eq_items:
                    bind = f"eq_{self._bind_idx}"
                    self._bind_idx += 1
                    self._binds[bind] = value
                    bind_expr = _apply_flags(f":{bind}", ci=True, trim=True)
                    clauses.append(f"{col_expr} = {bind_expr}")

                for value in prefix_items:
                    bind = f"eq_{self._bind_idx}"
                    self._bind_idx += 1
                    pattern = value
                    if not pattern.endswith("%"):
                        pattern = f"{pattern}%"
                    self._binds[bind] = pattern
                    bind_expr = _apply_flags(f":{bind}", ci=True, trim=True)
                    clauses.append(f"{col_expr} LIKE {bind_expr}")

                for value in contains_items:
                    bind = f"eq_{self._bind_idx}"
                    self._bind_idx += 1
                    pattern = value
                    if not pattern.startswith("%"):
                        pattern = f"%{pattern}"
                    if not pattern.endswith("%"):
                        pattern = f"{pattern}%"
                    self._binds[bind] = pattern
                    bind_expr = _apply_flags(f":{bind}", ci=True, trim=True)
                    clauses.append(f"{col_expr} LIKE {bind_expr}")

                if clauses:
                    self._where_parts.append("(" + " OR ".join(clauses) + ")")
                    continue
            values: List[Any] = []
            if isinstance(filt.get("values"), (list, tuple)):
                values.extend(filt.get("values") or [])
            else:
                val = filt.get("val")
                if val is not None:
                    values.append(val)
            if not values:
                continue
            op = str(filt.get("op") or "eq").lower()
            key = (column, ci, tr, op)
            buckets.setdefault(key, []).extend(values)
        for (column, ci, tr, op), values in buckets.items():
            names: List[str] = []
            unique_vals: List[Any] = []
            seen: set[Any] = set()
            for value in values:
                if value in seen:
                    continue
                seen.add(value)
                unique_vals.append(value)
            if not unique_vals:
                continue
            if op == "like":
                clauses: List[str] = []
                for value in unique_vals:
                    bind = f"eq_{self._bind_idx}"
                    self._bind_idx += 1
                    bind_val = value
                    if isinstance(bind_val, str) and "%" not in bind_val:
                        bind_val = f"%{bind_val}%"
                    self._binds[bind] = bind_val
                    left = _apply_flags(column, ci=ci, trim=tr)
                    right = _apply_flags(f":{bind}", ci=ci, trim=tr)
                    clauses.append(f"{left} LIKE {right}")
                if clauses:
                    self._where_parts.append("(" + " OR ".join(clauses) + ")")
                continue
            in_items: List[str] = []
            for value in unique_vals:
                bind = f"eq_{self._bind_idx}"
                self._bind_idx += 1
                self._binds[bind] = value
                in_expr = _apply_flags(f":{bind}", ci=ci, trim=tr)
                in_items.append(in_expr)
            column_expr = _apply_flags(column, ci=ci, trim=tr)
            if in_items:
                clause = f"{column_expr} IN (" + ", ".join(in_items) + ")"
                self._where_parts.append(clause)
        if buckets:
            self._notes.append(f"eq_filters={len(buckets)}")
        return self

    def apply_boolean_groups(self, groups: Iterable[Dict[str, Any]]) -> "QueryBuilder":
        or_groups: List[str] = []
        total_binds = 0
        for group in groups or []:
            if not isinstance(group, dict):
                continue
            fields = group.get("fields")
            if not isinstance(fields, list):
                continue
            and_fields: List[str] = []
            for field in fields:
                if not isinstance(field, dict):
                    continue
                values = [v for v in field.get("values") or [] if v is not None]
                if not values:
                    continue
                columns = field.get("expanded_columns") or [field.get("field")]
                columns = [
                    _normalize_identifier(str(col)) for col in columns if col
                ]
                columns = [col for col in columns if col]
                if not columns:
                    continue
                in_names: List[str] = []
                for value in values:
                    bind = f"eq_bg_{self._bind_idx}"
                    self._bind_idx += 1
                    self._binds[bind] = value
                    in_names.append(f"UPPER(TRIM(:{bind}))")
                if not in_names:
                    continue
                in_list = ", ".join(in_names)
                per_column = [
                    f"UPPER(TRIM({col})) IN ({in_list})" for col in columns
                ]
                if len(per_column) == 1:
                    and_fields.append(per_column[0])
                else:
                    and_fields.append("(" + " OR ".join(per_column) + ")")
                total_binds += len(in_names)
            if and_fields:
                or_groups.append("(" + " AND ".join(and_fields) + ")")
        if or_groups:
            self._where_parts.append("(" + " OR ".join(or_groups) + ")")
            self._notes.append(f"boolean_groups={total_binds}")
        return self

    def group_by(self, columns: Iterable[str] | str, *, gross: bool = False) -> "QueryBuilder":
        if isinstance(columns, str):
            columns = [columns]
        cleaned = [_normalize_identifier(str(col)) for col in columns if col]
        cleaned = [col for col in cleaned if col]
        if cleaned:
            self._group_by = cleaned
            self._gross = bool(gross)
            self._notes.append(
                f"group_by={','.join(self._group_by)} gross={self._gross}".strip()
            )
        return self

    def wants_all_columns(self, flag: bool) -> "QueryBuilder":
        self._wants_all_columns = bool(flag)
        return self

    def order_by(self, column: Optional[str], *, desc: bool = True) -> "QueryBuilder":
        normalized = _normalize_identifier(column) if column else None
        if not normalized:
            normalized = self.date_col
        self._order = _OrderConfig(column=normalized, descending=bool(desc))
        return self

    def limit(self, top_n: Optional[int]) -> "QueryBuilder":
        if isinstance(top_n, int) and top_n > 0:
            self._limit = top_n
            self._notes.append(f"limit={top_n}")
        else:
            self._limit = None
        return self

    # ------------------------------------------------------------------
    # Compilation helpers
    # ------------------------------------------------------------------
    def _build_select_clause(self) -> str:
        if self._group_by:
            group_sql = ", ".join(self._group_by)
            if self._gross:
                return (
                    f"{group_sql} AS GROUP_KEY, SUM({GROSS_EXPR}) AS TOTAL_GROSS, COUNT(*) AS CNT"
                )
            return f"{group_sql} AS GROUP_KEY, COUNT(*) AS CNT"
        if self._wants_all_columns:
            return "*"
        return "*"

    def _build_group_clause(self) -> str:
        if not self._group_by:
            return ""
        group_sql = ", ".join(self._group_by)
        return f" GROUP BY {group_sql}"

    def _build_order_clause(self) -> str:
        column = self._order.column or self.date_col
        direction = "DESC" if self._order.descending else "ASC"
        return f" ORDER BY {column} {direction}"

    def _apply_fts(self, where_parts: List[str], binds: Dict[str, Any]) -> None:
        if not (self._fts_engine and self._fts_columns and self._fts_groups):
            return
        builder = getattr(self._fts_engine, "build", None)
        if not callable(builder):
            return
        fts_sql, fts_binds = builder(
            self._fts_groups, self._fts_columns, self._fts_operator
        )
        if not fts_sql:
            return
        where_parts.insert(0, fts_sql)
        binds.update(fts_binds or {})

    def compile(self) -> tuple[str, Dict[str, Any]]:
        binds = dict(self._binds)
        where_parts = list(self._where_parts)
        self._apply_fts(where_parts, binds)
        where_sql = ""
        if where_parts:
            where_sql = " WHERE " + " AND ".join(where_parts)
        select_clause = self._build_select_clause()
        from_clause = f" FROM {_quote_table(self.table)}"
        group_clause = self._build_group_clause()
        order_clause = self._build_order_clause()
        sql = f"SELECT {select_clause}{from_clause}{where_sql}{group_clause}{order_clause}"
        if self._limit:
            sql = f"{sql}\nFETCH FIRST {self._limit} ROWS ONLY"
        return sql, binds

    def debug_info(self) -> Dict[str, Any]:
        return {"notes": list(self._notes)}
