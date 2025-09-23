from __future__ import annotations

from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - typing only
    from apps.dw.intent_legacy import DWIntent


def _table_literal(table: str) -> str:
    literal = (table or '"Contract"').strip()
    if not literal:
        return '"Contract"'
    if not (literal.startswith('"') and literal.endswith('"')):
        literal = f'"{literal.strip("\"")}"'
    return literal


def measure_sql(measure: str) -> str:
    if measure == "net":
        return "SUM(NVL(CONTRACT_VALUE_NET_OF_VAT, 0))"
    return "SUM(NVL(CONTRACT_VALUE_NET_OF_VAT, 0) + NVL(VAT, 0))"


def compose_sql(intent: "DWIntent", *, table: str = '"Contract"') -> str:
    table_name = _table_literal(table)
    where_clauses: list[str] = []
    if intent.window_key:
        where_clauses.append(f"{intent.date_column} BETWEEN :date_start AND :date_end")

    where_sql = ""
    if where_clauses:
        where_sql = "\nWHERE " + " AND ".join(where_clauses)

    if intent.agg == "count" and not intent.dimension:
        return f"SELECT\n  COUNT(*) AS CNT\nFROM {table_name}{where_sql}"

    if intent.dimension:
        if intent.agg == "count":
            metric = "COUNT(*)"
            alias = "CNT"
        else:
            metric = measure_sql(intent.measure)
            alias = "TOTAL_VALUE"
        sql = [
            "SELECT",
            f"  {intent.dimension} AS GROUP_KEY,",
            f"  {metric} AS {alias}",
            f"FROM {table_name}",
        ]
        if where_sql:
            sql.append(where_sql.strip())
        sql.append(f"GROUP BY {intent.dimension}")
        sql.append(f"ORDER BY {alias} DESC")
        if intent.user_requested_top_n:
            sql.append("FETCH FIRST :top_n ROWS ONLY")
        return "\n".join(sql)

    select_clause = "SELECT *"
    sql_lines = [select_clause, f"FROM {table_name}"]
    if where_sql:
        sql_lines.append(where_sql.strip())
    if intent.user_requested_top_n:
        sql_lines.append(f"ORDER BY {intent.date_column} DESC")
        sql_lines.append("FETCH FIRST :top_n ROWS ONLY")
    return "\n".join(sql_lines)
