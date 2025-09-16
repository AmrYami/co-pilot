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

    def plan(self, question: str, context: Dict[str, Any], hints: Dict[str, Any] | None = None) -> Tuple[str, str]:
        """
        Metric-first (if configured) else prompt the LLM with tables/columns/metrics.
        - If metric is recognized, start from its calculation_sql and inject date filters when present.
        """
        # metric-first?
        from os import getenv
        ask_mode = (context.get("ask_mode") or getenv("ASK_MODE") or "metric_first").lower()
        metrics: Dict[str, dict] = context.get("metrics") or {}

        if ask_mode == "metric_first" and metrics:
            try:
                # ask FA adapter to pick a metric and a date column
                from apps.fa.adapters import match_metric_key, default_date_column_for_metric
                mk = match_metric_key(question, metrics)
                if mk and mk in metrics:
                    base_sql = (metrics[mk].get("calculation_sql") or "").strip()
                    if base_sql:
                        # date hints?
                        if hints and hints.get("date_range"):
                            from core.sql_utils import inject_between_date_filter
                            dr = hints["date_range"]
                            # prefer a fully-qualified date col if we can
                            date_col = default_date_column_for_metric(mk) or ""
                            if date_col:
                                base_sql = inject_between_date_filter(
                                    base_sql, date_col, dr["start"].isoformat(), dr["end"].isoformat()
                                )
                        why = f"Used metric '{mk}' from registry; applied date filter if provided."
                        return base_sql, why
            except Exception:
                # fall back to LLM flow on any adapter error
                pass

        # fall back: LLM prompt using tables/columns
        tables = ", ".join(sorted({t['table_name'] for t in context.get('tables', [])}))
        cols = ", ".join(sorted({f"{c['table_name']}.{c['column_name']}" for c in context.get('columns', [])}))
        metrics_list = ", ".join(sorted((context.get("metrics") or {}).keys())) or "(none)"

        hint_txt = ""
        if hints:
            if dr := hints.get("date_range"):
                hint_txt += f"DateRange: {dr['start']}..{dr['end']} grain={dr.get('grain','day')}\n"
            if eqs := hints.get("eq_filters"):
                hint_txt += "Filters: " + ", ".join([f"{k}={v}" for k,v in eqs.items()]) + "\n"

        prompt = (
            "You are an expert SQL planner. Use ONLY the given tables/columns. "
            "Prefer metrics when they match the question.\n"
            f"Tables: {tables}\nColumns: {cols}\n"
            f"Metrics: {metrics_list}\n"
            f"Hints:\n{hint_txt if hint_txt else '(none)'}\n"
            "Rules:\n- Use JOINs as needed for filters on other tables.\n"
            "- Respect the date range; default to invoice/tran_date for sales when unsure.\n"
            "- Return canonical SQL with UNQUALIFIED table names and a short rationale.\n"
            "Return as:\nSQL:\n<sql>\nRationale:\n<why>\n"
        )
        out = self.llm.generate(prompt, max_new_tokens=256, temperature=0.2, top_p=0.9)
        return self._split(out)

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

    def plan(self, question: str, context: Dict[str, Any], hints: Dict[str, Any] | None = None) -> Tuple[str, str]:

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

    def fallback_clarifying_question(
        self,
        question: str,
        context: Dict[str, Any],
        hints: Dict[str, Any] | None = None,
    ) -> List[str]:
        return [
            "I couldn't derive a clean SQL. Can you clarify the tables, filters, or date range?"
        ]

class ValidatorAgent:
    def __init__(self, fa_engine: Optional[Engine], settings: Optional["Settings"]=None) -> None:
        """
                Lightweight safety validator.
                - If VALIDATE_WITH_EXPLAIN_ONLY=true → only EXPLAIN the query.
                - Else → EXPLAIN, then try probing with SELECT * FROM (<sql>) t LIMIT 1.
                """
        self.fa = fa_engine
        self.settings = settings

    def quick_validate(self, sql: str) -> Tuple[bool, Dict[str, Any]]:
        # Step 0: presence and shape
        if not self.fa:
            return False, {"error": "FA engine not configured"}
        sql_strip = sql.strip().lstrip("(")
        import re
        if not re.match(r"(?is)^(with|select)\b", sql_strip):
            return False, {"error": "planner returned non-SELECT text", "preview": sql_strip[:160]}

        explain_only = True
        if self.settings is not None:
            try:
                explain_only = bool(self.settings.get("VALIDATE_WITH_EXPLAIN_ONLY", True))
            except Exception:
                pass

        # Step 1: EXPLAIN
        try:
            with self.fa.connect() as c:
                c.execute(text(f"EXPLAIN {sql_strip}"))
        except Exception as e:
            return False, {"error": "explain_failed", "details": str(e)}

        # Step 2: Optional probe with LIMIT 1
        if explain_only:
            return True, {"message": "EXPLAIN ok"}

        try:
            probe = f"SELECT * FROM ( {sql_strip} ) t LIMIT 1"
            with self.fa.connect() as c:
                c.execute(text(probe))
            return True, {"message": "EXPLAIN+PROBE ok"}
        except Exception as e:
            # classify common errors to speed up fixes
            msg = str(e).lower()
            if "unknown column" in msg or "doesn't exist" in msg:
                cat = "schema"
            elif "syntax" in msg:
                cat = "syntax"
            elif "permission" in msg or "access denied" in msg:
                cat = "permission"
            else:
                cat = "runtime"
            return False, {"error": "probe_failed", "category": cat, "details": str(e)}
