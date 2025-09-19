"""DocuWare SQL generation helper using the shared SQLCoder model."""

from __future__ import annotations

import json
import re
from typing import Any, Dict, Optional

from core.model_loader import get_model, load_llm


# ---------------------------------------------------------------------------
# Clarifier helpers
# ---------------------------------------------------------------------------

CLARIFIER_SYSTEM = """You are a careful analyst. 
Return a single compact JSON object describing the user's requested intent over the DocuWare `Contract` table.
Schema highlights:
- Monetary: CONTRACT_VALUE_NET_OF_VAT (number), VAT (number), GROSS = NVL(CONTRACT_VALUE_NET_OF_VAT,0)+NVL(VAT,0)
- Dates: REQUEST_DATE, START_DATE, END_DATE, EXPIERY_30/60/90
- Stakeholders/Departments: CONTRACT_STAKEHOLDER_1..8 paired with DEPARTMENT_1..8
- Owner department: OWNER_DEPARTMENT, and DEPARTMENT_OUL is the org lead

JSON fields to output (only these):
{
  "intent": "select" | "rank" | "count" | "sum" | "avg",
  "entity": "contracts" | "stakeholders" | "departments",
  "time": {"column": "REQUEST_DATE"|"START_DATE"|"END_DATE", "range": {"type": "last_month"|"last_90_days"|"...", "start": "YYYY-MM-DD", "end": "YYYY-MM-DD"}},
  "group_by": ["stakeholder"|"department"|"owner_department"|"..."],
  "metrics": ["gross_value","count_contracts", "..."],
  "top_n": 10
}
If unclear, infer sensible defaults (REQUEST_DATE for generic “last month”, stakeholder view for stakeholder questions).
Return ONLY the JSON, no prose.
"""


def _clarifier_fallback(reason: str) -> Dict[str, Any]:
    return {
        "intent": "select",
        "table": "Contract",
        "metric": "contract_value_gross",
        "date_window": "auto",
        "time": {"range": {"type": "auto", "start": None, "end": None}},
        "filters": [],
        "group_by": [],
        "top_n": None,
        "confidence": 0.0,
        "reason": reason,
    }


def clarify_intent(question: str, context: Dict[str, Any]) -> Dict[str, Any]:
    try:
        mdl = get_model("clarifier")
    except Exception as exc:
        return _clarifier_fallback(f"clarifier unavailable ({type(exc).__name__})")

    if not mdl:
        return _clarifier_fallback("clarifier disabled")

    user = f"Question: {question}\nContext: {json.dumps(context, ensure_ascii=False)}"
    try:
        out = mdl.generate(
            system_prompt=CLARIFIER_SYSTEM,
            user_prompt=user,
            max_new_tokens=256,
        )
    except Exception as exc:
        return _clarifier_fallback(f"clarifier error ({type(exc).__name__})")

    if not out:
        return _clarifier_fallback("clarifier empty output")

    try:
        start = out.find("{")
        end = out.rfind("}")
        if start != -1 and end != -1 and end > start:
            parsed = json.loads(out[start : end + 1])
        else:
            return _clarifier_fallback("clarifier malformed output")
    except Exception:
        return _clarifier_fallback("clarifier parse error")

    if not isinstance(parsed, dict):
        return _clarifier_fallback("clarifier non-dict output")

    try:
        confidence = float(parsed.get("confidence", 0.0) or 0.0)
    except Exception:
        confidence = 0.0

    if confidence < 0.3:
        return _clarifier_fallback("clarifier low confidence")

    parsed.setdefault("intent", "select")
    parsed.setdefault("table", "Contract")
    parsed.setdefault("filters", [])
    parsed.setdefault("group_by", [])
    parsed.setdefault("metric", "contract_value_gross")
    parsed.setdefault("time", {"range": {"type": "auto"}})
    parsed["confidence"] = confidence
    return parsed


# ---------------------------------------------------------------------------
# SQL generation helpers
# ---------------------------------------------------------------------------

_EXPIRY_HINTS = re.compile(
    r"\b(expir|expiry|expiring|end[-\s]?date|ending|renew(al|ing)?)\b", re.IGNORECASE
)

_SQL_FENCE = re.compile(r"```(?:sql)?\s*([\s\S]*?)```", re.IGNORECASE)
_SQL_BLOCK = re.compile(r"(?is)\b(with|select)\b[\s\S]+")


def choose_date_column(user_text: str, default_col: str) -> str:
    """Return END_DATE when the question clearly refers to expirations."""

    return "END_DATE" if _EXPIRY_HINTS.search(user_text or "") else default_col


def extract_sql_only(text: str) -> Optional[str]:
    """Extract the first SELECT/CTE statement from raw LLM output."""

    if not text:
        return None

    candidate = text
    fence = _SQL_FENCE.search(candidate)
    if fence:
        candidate = fence.group(1)

    candidate = candidate.strip()
    if candidate.lower().startswith("sql:"):
        candidate = candidate[4:].strip()

    block = _SQL_BLOCK.search(candidate)
    if not block:
        return None

    sql = block.group(0).strip()
    sql = sql.split("```", 1)[0].strip()
    sql = sql.rstrip(";")

    upper = sql.lstrip().upper()
    if upper.startswith("WITH") or upper.startswith("SELECT"):
        return sql
    return None


def _default_columns() -> list[str]:
    return [
        "CONTRACT_ID",
        "CONTRACT_OWNER",
        "CONTRACT_STAKEHOLDER_1",
        "CONTRACT_STAKEHOLDER_2",
        "CONTRACT_STAKEHOLDER_3",
        "CONTRACT_STAKEHOLDER_4",
        "CONTRACT_STAKEHOLDER_5",
        "CONTRACT_STAKEHOLDER_6",
        "CONTRACT_STAKEHOLDER_7",
        "CONTRACT_STAKEHOLDER_8",
        "DEPARTMENT_1",
        "DEPARTMENT_2",
        "DEPARTMENT_3",
        "DEPARTMENT_4",
        "DEPARTMENT_5",
        "DEPARTMENT_6",
        "DEPARTMENT_7",
        "DEPARTMENT_8",
        "OWNER_DEPARTMENT",
        "CONTRACT_VALUE_NET_OF_VAT",
        "VAT",
        "CONTRACT_PURPOSE",
        "CONTRACT_SUBJECT",
        "START_DATE",
        "END_DATE",
        "REQUEST_DATE",
        "REQUEST_TYPE",
        "CONTRACT_STATUS",
        "ENTITY_NO",
        "REQUESTER",
    ]


def build_dw_prompt(
    question: str,
    ctx: Dict[str, Any],
    *,
    intent: Optional[Dict[str, Any]] = None,
) -> str:
    """Return an Oracle-specific instruction prompt for SQLCoder."""

    table = ctx.get("contract_table", "Contract")
    date_col = ctx.get("date_column", "REQUEST_DATE")
    columns = ctx.get("columns") or _default_columns()
    cols = ", ".join(columns)

    prompt_lines = [
        "-- ROLE: Convert natural language into a single valid Oracle SQL query.",
        "-- RULES:",
        "-- 1) Output ONLY one SELECT/CTE statement (no comments, no explanations, no prose).",
        f"-- 2) Use table \"{table}\" and these columns only: {cols}.",
        f"-- 3) If a time window is implied, filter using this column: {date_col}.",
        "-- 4) Use named binds :date_start and :date_end for time windows (BETWEEN :date_start AND :date_end or >= :date_start AND < :date_end).",
        "-- 5) Use Oracle syntax: NVL(), LISTAGG(... WITHIN GROUP (...)), FETCH FIRST N ROWS ONLY, TRIM(), UPPER(), etc.",
        "-- 6) Never modify data. No DML/DDL. SELECT/CTE only.",
        "",
        "-- QUESTION:",
        question,
    ]

    if intent:
        intent_json = json.dumps(intent, ensure_ascii=False)
        prompt_lines.extend(
            ["", "-- CLARIFIED INTENT:"]
            + [f"-- {line}" for line in intent_json.splitlines()]
        )

    prompt_lines.append("-- SQL:")
    return "\n".join(prompt_lines)


def nl_to_sql_with_llm(
    question: Optional[str] = None,
    *,
    intent: Optional[Dict[str, Any]] = None,
    settings: Optional[Any] = None,
    dw_table: str = "Contract",
    context: Optional[Dict[str, Any]] = None,
) -> Dict[str, object]:
    """Use SQLCoder to translate natural language or structured intent into Oracle SQL."""

    llm = load_llm("sql")
    if not llm:
        return {"sql": None, "confidence": 0.0, "why": "sql_model_unavailable"}

    generator = llm.get("handle")
    if generator is None:
        return {"sql": None, "confidence": 0.0, "why": "sql_generator_missing"}

    if not question and not intent:
        return {"sql": None, "confidence": 0.0, "why": "missing_prompt"}

    ctx: Dict[str, Any] = dict(context or {})
    if dw_table:
        ctx.setdefault("contract_table", dw_table)
    else:
        ctx.setdefault("contract_table", "Contract")

    default_col = ctx.get("date_column") or "REQUEST_DATE"
    if settings is not None and "date_column" not in ctx:
        try:
            default_col = settings.get(
                "DW_DATE_COLUMN", default=default_col, scope="namespace"
            )
        except Exception:
            default_col = default_col

    if intent:
        time_block = (intent or {}).get("time") or {}
        column = time_block.get("column")
        if column:
            default_col = column

    ctx["date_column"] = choose_date_column(question or "", default_col)

    prompt = build_dw_prompt(question or "", ctx, intent=intent)

    try:
        raw_text = generator.generate(prompt)
    except Exception as exc:
        return {
            "sql": None,
            "confidence": 0.0,
            "why": f"generator_error: {exc}",
            "raw": None,
            "used_date_column": ctx.get("date_column"),
        }

    sql = extract_sql_only(raw_text)
    if not sql:
        return {
            "sql": None,
            "confidence": 0.0,
            "why": "non_sql_output",
            "raw": raw_text or "",
            "used_date_column": ctx.get("date_column"),
        }

    lowered = sql.lower()
    if any(
        token in lowered
        for token in (" delete ", " update ", " insert ", " drop ", " alter ", " truncate ")
    ):
        return {
            "sql": None,
            "confidence": 0.0,
            "why": "unsafe_sql",
            "raw": raw_text or "",
            "used_date_column": ctx.get("date_column"),
        }

    confidence = 0.82 if intent else 0.75
    return {
        "sql": sql,
        "confidence": confidence,
        "why": "ok",
        "raw": raw_text or "",
        "used_date_column": ctx.get("date_column"),
    }
