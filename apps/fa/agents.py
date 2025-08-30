"""
FA-specific agents that compose the core scaffolds with domain prompts.
This file can evolve independently from the core.
"""
from __future__ import annotations

from typing import Any, Dict, Iterable, List, Tuple


from core.agents import ClarifierAgent as CoreClarifier, PlannerAgent as CorePlanner, ValidatorAgent as CoreValidator
from apps.fa.adapters import match_metric, parse_date_range, inject_date_filter, union_for_prefixes


class ClarifierAgentFA(CoreClarifier):
    def __init__(self, llm_handle, settings):
        super().__init__(llm_handle)
        self.settings = settings
    """
        ClarifierAgentFA
        ----------------
        Purpose:
          Decide whether to ask follow-up questions *based on policy* and context richness.

        Policy (ASK_MODE read from Settings; default by ENVIRONMENT):
          - "metric_first": if a metric alias matches the question → **skip asking** and let planner proceed.
          - "always_ask": always ask when context is weak/ambiguous.
          - "never_ask": never ask—proceed straight to planning.

        Inputs:
          - question: raw user question (str)
          - context: dict with tables/columns/metrics populated by Pipeline.build_context_pack()

        Output:
          - (need: bool, questions: List[str])

        Steps:
          1) Read ASK_MODE from settings with sane defaults.
          2) If "never_ask" → skip questions.
          3) If "metric_first" and a metric matches → skip questions.
          4) Otherwise, ask 0–2 lightweight questions when context looks ambiguous.
        """
    def __init__(self, llm_handle: Any, settings: Any) -> None:
        self.llm = llm_handle
        self.settings = settings

    def maybe_ask(self, question: str, context: Dict[str, Any]) -> Tuple[bool, List[str]]:
        # 1) Read policy; default to metric_first in dev, always_ask in prod.
        env = (self.settings.get("ENVIRONMENT", "local") or "local").lower()
        default_mode = "metric_first" if env in ("local", "dev", "development") else "always_ask"
        ask_mode = (self.settings.get("ASK_MODE", default_mode) or default_mode).lower()

        # 2) Short-circuit when policy says "never_ask"
        if ask_mode == "never_ask":
            return False, []  # proceed directly to planning

        # 3) If policy is metric_first and the question clearly matches a metric, don't ask
        if ask_mode == "metric_first":
            metrics = context.get("metrics", {}) or {}
            if match_metric(question, metrics):
                return False, []  # proceed; planner will use the metric

        # 4) Heuristic: ask when context is ambiguous (few/no tables, many columns, or date hint)
        qs: List[str] = []
        tables = context.get("tables", [])
        columns = context.get("columns", [])
        if not tables:
            qs.append("Should we use sales/invoices (debtor_trans), customers (debtors_master), or GL?")
        if any("date" in (c.get("column_name","").lower()) for c in columns):
            qs.append("What date range should we use?")
        need = len(qs) > 0
        return need, qs[:2]  # keep it short



class PlannerAgentFA(CorePlanner):
    def __init__(self, llm_handle, settings):
        super().__init__(llm_handle)
        self.settings = settings
    """
       PlannerAgentFA
       --------------
       Purpose:
         Prefer semantic metrics when available; fall back to core planner otherwise.

       Steps:
         1) Try to match a metric key/alias from context.metrics.
         2) If matched, optionally inject a date predicate (last month, ytd, etc.).
         3) Return canonical (unprefixed) SQL + rationale for the Validator.
         4) Else, fall back to the core planner prompt.
       """

    def plan(self, question: str, context: Dict[str, Any], hints: Dict[str, Any] | None = None) -> Tuple[str, str]:

        tables = ", ".join(sorted({t['table_name'] for t in context.get('tables', [])}))
        cols = ", ".join(sorted({f"{c['table_name']}.{c['column_name']}" for c in context.get('columns', [])}))
        metrics = context.get("metrics", {})

        hint_txt = ""
        if hints:
            if dr := hints.get("date_range"):
                hint_txt += f"DateRange: {dr['start']}..{dr['end']} grain={dr.get('grain','day')}\n"
            if eqs := hints.get("eq_filters"):
                hint_txt += "Filters: " + ", ".join([f"{k}={v}" for k,v in eqs.items()]) + "\n"

        prompt = (
            "You are an expert SQL planner over a FrontAccounting-like schema. "
            "Use ONLY the given tables/columns. Prefer metrics when they match the question.\n"
            f"Tables: {tables}\nColumns: {cols}\n"
            f"Metrics: {', '.join(metrics.keys()) if metrics else '(none)'}\n"
            f"Hints:\n{hint_txt if hint_txt else '(none)'}\n"
            "Rules:\n- Use JOINs as needed for filters that reference columns on other tables.\n"
            "- Respect the date range if supplied; default to invoice/tran_date for sales when unsure.\n"
            "- Return canonical SQL with UNQUALIFIED table names (no prefixes) and a short rationale.\n"
            "Return as:\nSQL:\n<sql>\nRationale:\n<why>\n"
        )
        out = self.llm.generate(prompt, max_new_tokens=256, temperature=0.2, top_p=0.9)
        return self._split(out)

    def _rule_based_plan(self, question: str, context: Dict[str, Any]) -> Tuple[str, str]:
        q = question.lower()
        # Heuristic for this very common ask
        if ("customer" in q or "customers" in q) and "sales" in q and ("last month" in q or "previous month" in q):
            sql = (
                "WITH bounds AS (\n"
                "  SELECT DATE_SUB(DATE_FORMAT(CURDATE(),'%Y-%m-01'), INTERVAL 1 MONTH) AS start_date,\n"
                "         DATE_FORMAT(CURDATE(),'%Y-%m-01') AS end_date\n"
                ")\n"
                "SELECT dm.name AS customer_name,\n"
                "       SUM(dt.ov_amount + dt.ov_gst) AS sales_amount\n"
                "FROM debtor_trans dt\n"
                "JOIN debtors_master dm ON dt.debtor_no = dm.debtor_no\n"
                "CROSS JOIN bounds b\n"
                "WHERE dt.type = 10\n"
                "  AND dt.tran_date >= b.start_date AND dt.tran_date < b.end_date\n"
                "GROUP BY dm.name\n"
                "ORDER BY sales_amount DESC\n"
                "LIMIT 10"
            )
            why = "Aggregate last month's sales (debtor_trans type=10) by debtor and return top 10."
            return sql, why
        # Default: force a clarifier up-stream
        return ("SELECT 1 /* no plan */", "No plan from model; clarifier should ask for tables/date range.")



class ValidatorAgentFA(CoreValidator):
    def quick_validate(self, sql: str) -> Tuple[bool, Dict[str, Any]]:
        sql_strip = sql.strip().lstrip("(")
        import re
        if not re.match(r"(?is)^(with|select|insert|update|delete|explain)\b", sql_strip):
            return False, {"error": "planner returned non-SQL text", "preview": sql_strip[:160]}
        return super().quick_validate(sql_strip)





def expand_sql_for_prefixes(canonical_sql: str, prefixes: Iterable[str]) -> str:
    return union_for_prefixes(canonical_sql, prefixes)
