from __future__ import annotations

import json
import re
from typing import Any, Dict

from core.model_loader import get_model

ALLOWED_COLUMNS = (
    "CONTRACT_ID, CONTRACT_OWNER, "
    "CONTRACT_STAKEHOLDER_1, CONTRACT_STAKEHOLDER_2, CONTRACT_STAKEHOLDER_3, CONTRACT_STAKEHOLDER_4, "
    "CONTRACT_STAKEHOLDER_5, CONTRACT_STAKEHOLDER_6, CONTRACT_STAKEHOLDER_7, CONTRACT_STAKEHOLDER_8, "
    "DEPARTMENT_1, DEPARTMENT_2, DEPARTMENT_3, DEPARTMENT_4, DEPARTMENT_5, DEPARTMENT_6, DEPARTMENT_7, DEPARTMENT_8, "
    "OWNER_DEPARTMENT, CONTRACT_VALUE_NET_OF_VAT, VAT, CONTRACT_PURPOSE, CONTRACT_SUBJECT, "
    "START_DATE, END_DATE, REQUEST_DATE, REQUEST_TYPE, CONTRACT_STATUS, ENTITY_NO, REQUESTER"
)

ALLOWED_BINDS = (
    "date_start",
    "date_end",
    "top_n",
    "owner_name",
    "dept",
    "entity_no",
    "contract_id_pattern",
    "request_type",
)

_FENCE_SQL = re.compile(r"```sql\s*(.*?)```", re.IGNORECASE | re.DOTALL)
_FENCE_ANY = re.compile(r"```+\s*(.*?)```+", re.IGNORECASE | re.DOTALL)
_START_SQL_ANCHORED = re.compile(r"(?mis)^\s*(SELECT|WITH)\b.*")


def extract_sql(text: str) -> str:
    """Extract the SQL portion from a model response."""

    if not text:
        return ""

    # 1) Prefer fenced ```sql blocks
    match = _FENCE_SQL.search(text)
    if match and match.group(1).strip():
        return match.group(1).strip()

    # 2) Any fenced block when the model omits `sql`
    match = _FENCE_ANY.search(text)
    if match and match.group(1).strip():
        return match.group(1).strip()

    # 3) Fallback to first anchored SELECT/WITH line
    match = _START_SQL_ANCHORED.search(text)
    if match:
        return match.group(0).strip()

    return ""


def clarify_intent(question: str, context: dict) -> Dict[str, Any]:
    """Run the clarifier model and augment with heuristic defaults."""

    mdl = get_model("clarifier")
    prompt = (
        "You are a precise NLU clarifier. Output JSON only.\n"
        "Keys:\n"
        "  has_time_window: boolean\n"
        "  date_column: string|null (END_DATE|REQUEST_DATE|START_DATE)\n"
        "  top_n: integer|null\n"
        "  explicit_dates: object|null {start,end} (ISO dates)\n"
        "Return JSON only between <<JSON>> and <</JSON>>.\n\n"
        f"Question: {question}\n\n<<JSON>>\n{{}}\n<</JSON>>\n"
    )
    raw = mdl.generate(prompt, max_new_tokens=192)
    raw_json = "{}"
    if raw:
        text = raw
        start = text.find("<<JSON>>")
        end = text.find("<</JSON>>")
        if start != -1 and end != -1 and end > start:
            raw_json = text[start + 8 : end].strip() or "{}"
    try:
        intent_data = json.loads(raw_json or "{}")
    except Exception:
        intent_data = {}

    intent: Dict[str, Any] = {
        "has_time_window": intent_data.get("has_time_window"),
        "date_column": intent_data.get("date_column"),
        "top_n": intent_data.get("top_n"),
        "explicit_dates": intent_data.get("explicit_dates"),
    }

    q_lower = (question or "").lower()
    date_terms = [
        "last month",
        "this month",
        "next 30 days",
        "last 30 days",
        "in 2024",
        "in 2025",
        "last quarter",
        "last 90 days",
    ]
    if any(term in q_lower for term in date_terms):
        intent.setdefault("has_time_window", True)

    if intent.get("has_time_window") and not intent.get("date_column"):
        if any(k in q_lower for k in ("end date", "end_date", "expiry", "expires")):
            intent["date_column"] = "END_DATE"
        elif any(k in q_lower for k in ("start date", "start_date")):
            intent["date_column"] = "START_DATE"
        else:
            intent["date_column"] = "REQUEST_DATE"

    match = re.search(r"\btop\s+(\d+)\b", q_lower)
    if match and not intent.get("top_n"):
        try:
            intent["top_n"] = int(match.group(1))
        except Exception:
            pass

    return {"ok": True, "used": True, "intent": intent, "raw": raw}


def build_sql_prompt(question: str, intent_bundle: Dict[str, Any], context: dict) -> str:
    """Construct the SQL generation prompt with guidance and example."""

    intent = intent_bundle.get("intent", {}) if intent_bundle else {}
    allowed_cols = ALLOWED_COLUMNS
    lines = [
        "Return Oracle SQL only in a fenced block:",
        "```sql",
        "-- Table: \"Contract\"",
        f"-- Allowed columns: {allowed_cols}",
        "-- Oracle only: NVL, LISTAGG ... WITHIN GROUP, TRIM, UPPER, FETCH FIRST N ROWS ONLY",
        "-- Never write DML/DDL",
        "-- Use only these named binds when needed: :date_start, :date_end, :top_n, :owner_name, :dept, :entity_no, :contract_id_pattern, :request_type",
    ]

    if intent.get("has_time_window"):
        column = intent.get("date_column") or "REQUEST_DATE"
        lines.append(f"-- Use :date_start and :date_end on {column} for the requested window")
    else:
        lines.append("-- Do not add a date filter unless explicitly requested")

    example = (
        "/* Example: top 3 stakeholders by gross value in a window */\n"
        "WITH stk AS (\n"
        "  SELECT CONTRACT_ID,\n"
        "         NVL(CONTRACT_VALUE_NET_OF_VAT,0)+NVL(VAT,0) AS CONTRACT_VALUE_GROSS,\n"
        "         CONTRACT_STAKEHOLDER_1 AS STAKEHOLDER,\n"
        "         REQUEST_DATE AS REF_DATE\n"
        "  FROM \"Contract\"\n"
        "  UNION ALL SELECT CONTRACT_ID, NVL(CONTRACT_VALUE_NET_OF_VAT,0)+NVL(VAT,0), CONTRACT_STAKEHOLDER_2, REQUEST_DATE FROM \"Contract\"\n"
        "  UNION ALL SELECT CONTRACT_ID, NVL(CONTRACT_VALUE_NET_OF_VAT,0)+NVL(VAT,0), CONTRACT_STAKEHOLDER_3, REQUEST_DATE FROM \"Contract\"\n"
        "  UNION ALL SELECT CONTRACT_ID, NVL(CONTRACT_VALUE_NET_OF_VAT,0)+NVL(VAT,0), CONTRACT_STAKEHOLDER_4, REQUEST_DATE FROM \"Contract\"\n"
        "  UNION ALL SELECT CONTRACT_ID, NVL(CONTRACT_VALUE_NET_OF_VAT,0)+NVL(VAT,0), CONTRACT_STAKEHOLDER_5, REQUEST_DATE FROM \"Contract\"\n"
        "  UNION ALL SELECT CONTRACT_ID, NVL(CONTRACT_VALUE_NET_OF_VAT,0)+NVL(VAT,0), CONTRACT_STAKEHOLDER_6, REQUEST_DATE FROM \"Contract\"\n"
        "  UNION ALL SELECT CONTRACT_ID, NVL(CONTRACT_VALUE_NET_OF_VAT,0)+NVL(VAT,0), CONTRACT_STAKEHOLDER_7, REQUEST_DATE FROM \"Contract\"\n"
        "  UNION ALL SELECT CONTRACT_ID, NVL(CONTRACT_VALUE_NET_OF_VAT,0)+NVL(VAT,0), CONTRACT_STAKEHOLDER_8, REQUEST_DATE FROM \"Contract\"\n"
        ")\n"
        "SELECT TRIM(STAKEHOLDER) AS STAKEHOLDER,\n"
        "       SUM(CONTRACT_VALUE_GROSS) AS TOTAL_GROSS_VALUE\n"
        "FROM stk\n"
        "WHERE STAKEHOLDER IS NOT NULL AND TRIM(STAKEHOLDER) <> ''\n"
        "  AND REF_DATE >= :date_start AND REF_DATE < :date_end\n"
        "GROUP BY TRIM(STAKEHOLDER)\n"
        "ORDER BY TOTAL_GROSS_VALUE DESC\n"
        "FETCH FIRST 3 ROWS ONLY"
    )
    lines.append(example)
    lines.append("")
    lines.append("-- Question:")
    lines.append(question)
    lines.append("")
    lines.append("-- Answer below:")
    lines.append("```sql")

    return "\n".join(lines)


def nl_to_sql_with_llm(question: str, context: dict) -> Dict[str, Any]:
    """Clarify, prompt, generate, and extract SQL."""

    intent = clarify_intent(question, context)
    prompt = build_sql_prompt(question, intent, context)
    mdl = get_model("sql")
    raw1 = mdl.generate(prompt, max_new_tokens=512, stop=["```"])
    sql1 = extract_sql(raw1 or "")
    return {
        "intent": intent,
        "prompt": prompt,
        "raw1": raw1,
        "sql1": sql1,
    }


def repair_sql(bad_sql: str, prompt_rules: str, question: str) -> Dict[str, Any]:
    """Run a repair pass using only the failing SQL."""

    mdl = get_model("sql")
    repair_prompt = (
        "Previous SQL had validation errors.\n"
        "Return Oracle SQL only inside a fenced block. No prose. No comments.\n"
        f"Question:\n{question}\n\n"
        f"Bad SQL:\n```sql\n{bad_sql or ''}\n```\n\n"
        "Fixed SQL:\n```sql\n"
    )
    raw2 = mdl.generate(repair_prompt, max_new_tokens=512, stop=["```"])
    sql2 = extract_sql(raw2 or "")
    return {"raw2": raw2, "sql2": sql2}


__all__ = [
    "ALLOWED_COLUMNS",
    "ALLOWED_BINDS",
    "build_sql_prompt",
    "clarify_intent",
    "extract_sql",
    "nl_to_sql_with_llm",
    "repair_sql",
]
