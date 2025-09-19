"""DocuWare SQL generation helper using the shared SQLCoder model."""

from __future__ import annotations

import json
import re
from typing import Any, Dict, Optional

from core.model_loader import get_model, load_llm

_SENTINEL_START = "BEGIN_SQL"
_SENTINEL_END = "END_SQL"


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
            max_new_tokens=96,
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

def choose_date_column(user_text: str, default_col: str) -> str:
    """Return END_DATE when the question clearly refers to expirations."""

    lowered = (user_text or "").lower()
    if "end_date" in lowered:
        return "END_DATE"
    if "start_date" in lowered:
        return "START_DATE"
    return "END_DATE" if _EXPIRY_HINTS.search(lowered) else default_col


def extract_sql_from_llm(raw: str) -> Optional[str]:
    """Extract SQL from the LLM output using sentinels or SELECT/WITH heuristics."""

    if not raw:
        return None

    sql = raw

    if _SENTINEL_START in raw and _SENTINEL_END in raw:
        sql = raw.split(_SENTINEL_START, 1)[1].split(_SENTINEL_END, 1)[0]
    elif _SENTINEL_START in raw:
        sql = raw.split(_SENTINEL_START, 1)[1]
    else:
        match = re.search(r"(?is)\b(SELECT|WITH)\b.*", raw)
        sql = match.group(0) if match else ""

    if not sql:
        return None

    sql = re.sub(r"```.*?```", "", sql, flags=re.S)
    lines = []
    for line in sql.splitlines():
        stripped = line.strip()
        if stripped.startswith("--"):
            continue
        lines.append(line)
    sql = "\n".join(lines).strip().rstrip(";")
    return sql or None


def ensure_date_window(sql: str, date_col: Optional[str], need_window: bool) -> str:
    """Ensure the generated SQL applies the expected date window binds."""

    if not sql or not need_window or not date_col:
        return sql
    if ":date_start" in sql or ":date_end" in sql:
        return sql

    clause = f"{date_col} >= :date_start AND {date_col} < :date_end"
    where_pattern = re.compile(r"(?is)\bWHERE\b")
    if where_pattern.search(sql):
        return where_pattern.sub(lambda m: f"{m.group(0)} {clause} AND", sql, count=1)

    insert_pattern = re.compile(r"(?is)\b(ORDER\s+BY|GROUP\s+BY|FETCH\s+FIRST|OFFSET|LIMIT)\b")
    match = insert_pattern.search(sql)
    insertion = f" WHERE {clause} "
    if match:
        idx = match.start()
        return sql[:idx].rstrip() + insertion + sql[idx:]
    return sql.rstrip() + insertion


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
    table: str,
    allowed_cols: list[str],
    date_hint: Optional[str],
    *,
    intent: Optional[Dict[str, Any]] = None,
) -> str:
    """Construct a compact instruction prompt that yields SQL only."""

    lowered = (question or "").lower()
    if "end_date" in lowered:
        date_hint = "END_DATE"
    elif "start_date" in lowered:
        date_hint = "START_DATE"

    cols_csv = ", ".join(allowed_cols)
    if date_hint:
        date_rule = (
            f"Filter time windows using {date_hint} with :date_start and :date_end binds."
        )
    else:
        date_rule = (
            "If a time window is implied, use :date_start and :date_end binds on the most relevant date column."
        )

    lines = [
        f"Return only valid Oracle SQL between {_SENTINEL_START} and {_SENTINEL_END}.",
        f"Table: \"{table}\".",
        f"Allowed columns: {cols_csv}.",
        date_rule,
        "Use Oracle SELECT or WITH statements only.",
        "Never include comments or explanations.",
        f"Question: {question}",
    ]

    if intent:
        intent_json = json.dumps(intent, ensure_ascii=False)
        lines.append(f"Intent: {intent_json}")

    lines.append(_SENTINEL_START)
    return "\n".join(lines) + "\n"


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

    columns = ctx.get("columns") or _default_columns()
    prompt = build_dw_prompt(
        question or "",
        ctx.get("contract_table", "Contract"),
        columns,
        ctx.get("date_column"),
        intent=intent,
    )

    try:
        raw_text = generator.generate(prompt, stop=[_SENTINEL_END])
    except Exception as exc:
        return {
            "sql": None,
            "confidence": 0.0,
            "why": f"generator_error: {exc}",
            "raw": None,
            "used_date_column": ctx.get("date_column"),
        }

    sql = extract_sql_from_llm(raw_text)
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
