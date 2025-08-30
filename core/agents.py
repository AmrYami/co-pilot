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
            qs.append("Is this about sales/invoices (debtor_trans), customers (debtors_master), or GL?")
            need = True
        if len(tables) > 3 or len(columns) > 10:
            qs.append("Which table should we focus on?")
            need = True
        if any("date" in c.get("column_name", "").lower() for c in columns):
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

    def plan(self, question: str, context: Dict[str, Any]) -> Tuple[str, str]:
        raise NotImplementedError

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
                # optional execution based on settings flag:
                try:
                    from flask import current_app
                    settings = current_app.config["PIPELINE"].settings
                    explain_only = bool(settings.get("VALIDATE_WITH_EXPLAIN_ONLY", True))
                except Exception:
                    explain_only = True

                if not explain_only:
                    # run a super-light probe
                    probe = f"SELECT * FROM ({sql_strip}) AS t LIMIT 1"
                    c.execute(text(probe))

            return True, {"message": "validation ok", "explain_only": explain_only}
        except Exception as e:
            return False, {"error": str(e)}
