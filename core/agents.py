# core/agents.py
from __future__ import annotations
import re, json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import text
from sqlalchemy.engine import Engine


@dataclass
class BaseContext:
    tables: List[Dict[str, Any]]
    columns: List[Dict[str, Any]]

class ClarifierAgent:
    def __init__(self, llm_handle: Any) -> None:
        self.llm = llm_handle

    def maybe_ask(self, question: str, context: Dict[str, Any]) -> Tuple[bool, List[str]]:
        tables = context.get("tables", [])
        columns = context.get("columns", [])
        qs: List[str] = []
        need = False
        if not tables:
            qs.append("I couldn't match tables. Which table should we use?")
            need = True
        if len(tables) > 3 or len(columns) > 10:
            qs.append("Which table should we focus on?")
            need = True
        if any("date" in (c.get("column_name","").lower()) for c in columns):
            qs.append("What date range should we use?")
            need = True
        return need, qs

class PlannerAgent:
    def __init__(self, llm_handle: Any) -> None:
        self.llm = llm_handle

    def _split(self, txt: str) -> Tuple[str, str]:
        lower = txt.lower()
        sql, why = "", ""
        if "sql:" in lower:
            i = lower.find("sql:")
            rest = txt[i+4:]
            j = rest.lower().find("rationale:")
            if j >= 0:
                sql = rest[:j].strip()
                why = rest[j+10:].strip()
            else:
                sql = rest.strip()
        else:
            sql = txt.strip()
        return sql, why

    def plan(
        self,
        question: str,
        context: Dict[str, Any],
        hints: Dict[str, Any] | None = None
    ) -> Tuple[str, str]:
        """Generic SQL planner over the provided schema context (app-agnostic)."""
        tables = ", ".join(sorted({t.get('table_name','') for t in context.get('tables', []) if t.get('table_name')}))
        cols = ", ".join(sorted({
            f"{c.get('table_name')}.{c.get('column_name')}"
            for c in context.get('columns', [])
            if c.get('table_name') and c.get('column_name')
        }))
        hint_txt = ""
        if hints:
            if (dr := hints.get("date_range")) and dr.get("start") and dr.get("end"):
                hint_txt += f"DateRange: {dr['start']}..{dr['end']} grain={dr.get('grain','day')}\n"
            if (eqs := hints.get("eq_filters")):
                hint_txt += "Filters: " + ", ".join([f"{k}={v}" for k,v in eqs.items()]) + "\n"

        prompt = (
            "You are an expert SQL planner. Use ONLY the given tables and columns.\n"
            f"Tables: {tables or '(none)'}\nColumns: {cols or '(none)'}\n"
            f"Hints:\n{hint_txt or '(none)'}\n"
            "Rules:\n- Use JOINs when filters reference columns on other tables.\n"
            "- Respect date ranges if provided.\n"
            "- Return canonical SQL with UNQUALIFIED table names (no prefixes) and a short rationale.\n"
            "Return as:\nSQL:\n<sql>\nRationale:\n<why>\n"
        )
        out = self.llm.generate(prompt, max_new_tokens=256, temperature=0.2, top_p=0.9)
        return self._split(out)

class ValidatorAgent:
    def __init__(self, fa_engine: Optional[Engine]) -> None:
        self.fa = fa_engine

    def quick_validate(self, sql: str) -> Tuple[bool, Dict[str, Any]]:
        if not self.fa:
            return False, {"error": "FA engine not configured"}
        sql_strip = sql.strip().lstrip("(")
        if not re.match(r"(?is)^(with|select|insert|update|delete|explain)\b", sql_strip):
            return False, {"error": "planner returned non-SQL text", "preview": sql_strip[:160]}
        try:
            with self.fa.connect() as c:
                c.execute(text(f"EXPLAIN {sql_strip}"))
            return True, {"message": "EXPLAIN ok"}
        except Exception as e:
            return False, {"error": str(e)}
