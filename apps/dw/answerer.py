from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Any, Dict, List, Optional, Sequence, Tuple

from sqlalchemy import text


class AnswerError(Exception):
    """Raised when a question cannot be handled by the rule-based answerer."""


@dataclass
class AnswerResult:
    sql: str
    rows: List[Dict[str, Any]]
    top_n: int
    date_start: datetime
    date_end: datetime
    tags: List[str]
    run_id: Optional[int] = None


_TOP_RE = re.compile(r"top\s+(?P<n>\d+)", re.IGNORECASE)
_SLOT_RE = re.compile(r"(\d+)$")


class StakeholderAnswerer:
    """Rule-based interpreter for DocuWare stakeholder aggregations."""

    METRIC_KEY = "contract_value_gross"

    def __init__(self, settings, mem_engine, registry) -> None:
        self.settings = settings
        self.namespace = getattr(settings, "namespace", "dw::common")
        self.mem = mem_engine
        self.registry = registry

    # ------------------------------------------------------------------
    def answer(self, question: str) -> AnswerResult:
        plan = self._interpret(question)
        if not plan:
            raise AnswerError("Question not recognised for DocuWare stakeholder metrics.")

        metric_expr = self._metric_expression()
        date_column = self._date_column()
        table_name = self._table_name()
        column_pairs = self._column_pairs()
        if not column_pairs:
            raise AnswerError("No stakeholder column mappings available for DocuWare.")

        sql_text = self._build_sql(metric_expr, date_column, table_name, column_pairs, plan["top_n"])
        rows = self._execute(sql_text, plan["start"], plan["end"])
        run_id = self._record_run(question, sql_text, rows)
        self._store_snippet(sql_text, plan["tags"])

        return AnswerResult(
            sql=sql_text,
            rows=rows,
            top_n=plan["top_n"],
            date_start=plan["start"],
            date_end=plan["end"],
            tags=plan["tags"],
            run_id=run_id,
        )

    # ------------------------------------------------------------------
    def _interpret(self, question: str) -> Optional[Dict[str, Any]]:
        text_q = question.lower()
        if "stakeholder" not in text_q:
            return None
        if not any(token in text_q for token in ("contract value", "gross", "value")):
            return None

        if "last month" in text_q or "previous month" in text_q:
            start, end = self._last_month_bounds()
            tags = ["dw", "contracts", "stakeholder", "last_month"]
        else:
            return None

        top_n = self._parse_top_n(text_q)
        tags.append(f"top_{top_n}")
        return {"top_n": top_n, "start": start, "end": end, "tags": tags}

    # ------------------------------------------------------------------
    def _parse_top_n(self, question: str) -> int:
        match = _TOP_RE.search(question)
        if match:
            try:
                value = int(match.group("n"))
                return max(1, min(value, 100))
            except Exception:
                pass
        if "top ten" in question:
            return 10
        return 10

    # ------------------------------------------------------------------
    def _last_month_bounds(self, today: Optional[date] = None) -> Tuple[datetime, datetime]:
        today = today or date.today()
        first_of_month = today.replace(day=1)
        last_month_end = first_of_month
        last_month_start = (first_of_month - timedelta(days=1)).replace(day=1)
        return (
            datetime.combine(last_month_start, time.min),
            datetime.combine(last_month_end, time.min),
        )

    # ------------------------------------------------------------------
    def _metric_expression(self) -> str:
        sql = text(
            """
            SELECT calculation_sql
              FROM mem_metrics
             WHERE namespace = :ns
               AND metric_key = :metric
          ORDER BY version DESC
             LIMIT 1
            """
        )
        with self.mem.connect() as conn:
            row = conn.execute(sql, {"ns": self.namespace, "metric": self.METRIC_KEY}).fetchone()
        if row and row[0]:
            return str(row[0])
        return "NVL(CONTRACT_VALUE_NET_OF_VAT,0) + NVL(VAT,0)"

    # ------------------------------------------------------------------
    def _date_column(self) -> str:
        column = self.settings.get_string("DW_DEFAULT_DATE_COLUMN", default="REQUEST_DATE")
        if column:
            return column
        return "REQUEST_DATE"

    # ------------------------------------------------------------------
    def _table_name(self) -> str:
        table_name = self.settings.get_string("DW_CONTRACT_TABLE", default="Contract")
        if not table_name:
            table_name = "Contract"
        if "." in table_name:
            return table_name
        if table_name.isupper() and table_name.replace("_", "").isalnum():
            return table_name
        return f'"{table_name}"'

    # ------------------------------------------------------------------
    def _column_pairs(self) -> List[Tuple[str, Optional[str]]]:
        stakeholders = self._fetch_column_aliases("stakeholder")
        departments = self._fetch_column_aliases("department")

        if not stakeholders:
            stakeholders = [f"CONTRACT_STAKEHOLDER_{idx}" for idx in range(1, 9)]
        if not departments:
            departments = [f"DEPARTMENT_{idx}" for idx in range(1, 9)]

        pairs: List[Tuple[str, Optional[str]]] = []
        for idx, stake_col in enumerate(stakeholders):
            dept_col = departments[idx] if idx < len(departments) else None
            pairs.append((stake_col, dept_col))
        return pairs

    # ------------------------------------------------------------------
    def _fetch_column_aliases(self, canonical: str) -> List[str]:
        sql = text(
            """
            SELECT alias
              FROM mem_mappings
             WHERE namespace = :ns
               AND canonical = :canon
               AND mapping_type = 'column'
          ORDER BY alias
            """
        )
        with self.mem.connect() as conn:
            rows = conn.execute(sql, {"ns": self.namespace, "canon": canonical}).fetchall()
        aliases = [str(row[0]) for row in rows if row and row[0]]
        return self._sort_by_slot(aliases)

    # ------------------------------------------------------------------
    def _sort_by_slot(self, columns: Sequence[str]) -> List[str]:
        def slot_key(name: str) -> int:
            match = _SLOT_RE.search(name or "")
            return int(match.group(1)) if match else 0

        return sorted(columns, key=slot_key)

    # ------------------------------------------------------------------
    def _build_sql(
        self,
        metric_expr: str,
        date_column: str,
        table_name: str,
        column_pairs: Sequence[Tuple[str, Optional[str]]],
        top_n: int,
    ) -> str:
        selects = []
        for stakeholder_col, dept_col in column_pairs:
            department_expr = dept_col if dept_col else "NULL"
            selects.append(
                """
                SELECT
                  CONTRACT_ID,
                  {metric} AS CONTRACT_VALUE_GROSS,
                  {stake} AS STAKEHOLDER,
                  {dept} AS DEPARTMENT,
                  {date_col} AS REF_DATE
                FROM {table}
                """.format(
                    metric=metric_expr,
                    stake=stakeholder_col,
                    dept=department_expr,
                    date_col=date_column,
                    table=table_name,
                ).strip()
            )

        union_sql = "\nUNION ALL\n".join(selects)
        limit_clause = f"FETCH FIRST {top_n} ROWS ONLY"

        return (
            """
            WITH stakeholders AS (
            {union}
            )
            SELECT
              TRIM(STAKEHOLDER) AS stakeholder,
              SUM(CONTRACT_VALUE_GROSS) AS total_gross_value,
              COUNT(DISTINCT CONTRACT_ID) AS contract_count,
              LISTAGG(DISTINCT TRIM(DEPARTMENT), ', ') WITHIN GROUP (ORDER BY TRIM(DEPARTMENT)) AS departments
            FROM stakeholders
            WHERE STAKEHOLDER IS NOT NULL
              AND TRIM(STAKEHOLDER) <> ''
              AND REF_DATE >= :date_start
              AND REF_DATE < :date_end
            GROUP BY TRIM(STAKEHOLDER)
            ORDER BY total_gross_value DESC
            {limit}
            """
        ).format(union=union_sql, limit=limit_clause).strip()

    # ------------------------------------------------------------------
    def _execute(self, sql_text: str, start: datetime, end: datetime) -> List[Dict[str, Any]]:
        engine = self.registry.engine(None)
        with engine.connect() as conn:
            result = conn.execute(
                text(sql_text),
                {"date_start": start, "date_end": end},
            ).mappings().all()
        return [dict(row) for row in result]

    # ------------------------------------------------------------------
    def _record_run(self, question: str, sql_text: str, rows: List[Dict[str, Any]]) -> Optional[int]:
        sample_json = json.dumps(rows[:5], default=str)
        attempts = [
            (
                """
                INSERT INTO mem_runs(namespace, run_type, input_query, sql_text, row_count, result_sample)
                VALUES (:ns, :rtype, :query, :sql, :count, CAST(:sample AS jsonb))
                RETURNING id
                """.strip(),
                {
                    "ns": self.namespace,
                    "rtype": "dw_rule",
                    "query": question,
                    "sql": sql_text,
                    "count": len(rows),
                    "sample": sample_json,
                },
            ),
            (
                """
                INSERT INTO mem_runs(namespace, input_query, sql_text, row_count)
                VALUES (:ns, :query, :sql, :count)
                RETURNING id
                """.strip(),
                {
                    "ns": self.namespace,
                    "query": question,
                    "sql": sql_text,
                    "count": len(rows),
                },
            ),
            (
                """
                INSERT INTO mem_runs(namespace, input_query, sql_text)
                VALUES (:ns, :query, :sql)
                RETURNING id
                """.strip(),
                {
                    "ns": self.namespace,
                    "query": question,
                    "sql": sql_text,
                },
            ),
        ]

        for stmt_text, params in attempts:
            try:
                with self.mem.begin() as conn:
                    row = conn.execute(text(stmt_text), params).fetchone()
                if row and row[0] is not None:
                    return int(row[0])
            except Exception:
                continue
        return None

    # ------------------------------------------------------------------
    def _store_snippet(self, sql_text: str, tags: Sequence[str]) -> None:
        table_label = self.settings.get_string("DW_CONTRACT_TABLE", default="Contract") or "Contract"
        payload = {
            "ns": self.namespace,
            "title": "Top stakeholders by gross contract value (last month)",
            "desc": "Rule-based DocuWare aggregation without LLM.",
            "tmpl": sql_text,
            "raw": sql_text,
            "input_tables": json.dumps([{"table": table_label}], default=str),
            "output_columns": json.dumps(
                ["stakeholder", "total_gross_value", "contract_count", "departments"],
                default=str,
            ),
            "tags": json.dumps(sorted(set(tags)), default=str),
            "source": "dw",
        }
        stmt = text(
            """
            INSERT INTO mem_snippets(namespace, title, description, sql_template, sql_raw,
                                     input_tables, output_columns, tags, is_verified, source)
            VALUES (:ns, :title, :desc, :tmpl, :raw,
                    CAST(:input_tables AS jsonb),
                    CAST(:output_columns AS jsonb),
                    CAST(:tags AS jsonb),
                    true,
                    :source)
            ON CONFLICT DO NOTHING
            """
        )
        try:
            with self.mem.begin() as conn:
                conn.execute(stmt, payload)
        except Exception:
            pass
