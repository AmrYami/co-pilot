import json
import os
import re
from datetime import date, timedelta

from core.model_loader import get_model

ALLOWED_COLS = (
    "CONTRACT_ID, CONTRACT_OWNER, "
    "CONTRACT_STAKEHOLDER_1, CONTRACT_STAKEHOLDER_2, CONTRACT_STAKEHOLDER_3, CONTRACT_STAKEHOLDER_4, "
    "CONTRACT_STAKEHOLDER_5, CONTRACT_STAKEHOLDER_6, CONTRACT_STAKEHOLDER_7, CONTRACT_STAKEHOLDER_8, "
    "DEPARTMENT_1, DEPARTMENT_2, DEPARTMENT_3, DEPARTMENT_4, DEPARTMENT_5, DEPARTMENT_6, DEPARTMENT_7, DEPARTMENT_8, "
    "OWNER_DEPARTMENT, CONTRACT_VALUE_NET_OF_VAT, VAT, CONTRACT_PURPOSE, CONTRACT_SUBJECT, "
    "START_DATE, END_DATE, REQUEST_DATE, REQUEST_TYPE, CONTRACT_STATUS, ENTITY_NO, REQUESTER"
)


def _extract_between(s: str, start: str, end: str) -> str | None:
    match = re.search(re.escape(start) + r"(.*?)" + re.escape(end), s, re.S)
    return match.group(1).strip() if match else None


def _clarifier_fallback(question: str) -> dict:
    lower = question.lower()
    intent = {
        "has_time_window": any(
            marker in lower
            for marker in ["last month", "next 30", "last 30", "in 202", "since", "between"]
        ),
        "date_column": None,
        "top_n": None,
        "explicit_dates": None,
    }

    if "end_date" in lower or "expiry" in lower or "expires" in lower:
        intent["date_column"] = "END_DATE"
    if "request date" in lower:
        intent["date_column"] = "REQUEST_DATE"
    if "start date" in lower:
        intent["date_column"] = "START_DATE"

    top_match = re.search(r"\btop\s+(\d+)\b", lower)
    if top_match:
        intent["top_n"] = int(top_match.group(1))

    if intent["has_time_window"] and not intent["date_column"]:
        intent["date_column"] = "REQUEST_DATE"

    return intent


def clarify_intent(question: str, ctx: dict | None = None) -> dict:
    model = get_model("clarifier")
    if not model:
        return _clarifier_fallback(question)

    prompt = (
        "You are a precise NLU clarifier. Output JSON only.\n"
        "Keys:\n"
        "  has_time_window: boolean\n"
        "  date_column: string|null (END_DATE|REQUEST_DATE|START_DATE)\n"
        "  top_n: integer|null\n"
        "  explicit_dates: object|null {start,end} (ISO dates)\n"
        "Return JSON only between <<JSON>> and <</JSON>>.\n\n"
        f"Question: {question}\n\n<<JSON>>\n{{}}\n<</JSON>>"
    )

    raw = model.generate(prompt, max_new_tokens=192, stop=["<</JSON>>"])
    blob = _extract_between(raw or "", "<<JSON>>", "<</JSON>>") or "{}"
    try:
        intent = json.loads(blob)
        intent = {**_clarifier_fallback(question), **intent}
    except Exception:
        intent = _clarifier_fallback(question)
    return intent


def _sql_prompt(question: str, intent: dict) -> str:
    has_window = bool(intent.get("has_time_window"))
    date_col = intent.get("date_column") or "REQUEST_DATE"

    head = (
        'Return only Oracle SQL inside a fenced block:\n```sql\n'
        '-- SQL only. No comments.\n'
    )
    rules = (
        '/* Table */\n'
        '/* "Contract" */\n'
        '/* Allowed columns */\n'
        f'/* {ALLOWED_COLS} */\n'
        '/* Oracle syntax: NVL, TRIM, LISTAGG WITHIN GROUP, FETCH FIRST N ROWS ONLY */\n'
        '/* No DML/DDL */\n'
        '/* Allowed binds: date_start, date_end, top_n, owner_name, dept, entity_no, contract_id_pattern, request_type */\n'
    )
    window = ""
    if has_window:
        window = f"/* Use {date_col} with :date_start and :date_end when filtering a time window */\n"

    tail = (
        f"\n/* Question */\n/* {question} */\n"
        "SELECT /* start with SELECT or WITH */\n"
    )

    return head + rules + window + tail


def _sql_repair_prompt(question: str, previous_sql: str, errors: list[str]) -> str:
    return (
        "Previous SQL had validation errors: " + ", ".join(errors) + "\n"
        "Repair the SQL. Return Oracle SQL only inside ```sql fenced block. No comments.\n"
        "Rules: table \"Contract\"; use only allowed columns; allowed binds only; Oracle syntax; SELECT/CTE only.\n\n"
        f"Question: {question}\n\n"
        "```sql\n" + previous_sql.strip() + "\n```"
    )


def _extract_sql_block(text: str) -> str:
    match = re.search(r"```sql\s*(.*?)\s*```", text or "", re.S | re.I)
    if not match:
        match = re.search(r"```\s*(.*?)\s*```", text or "", re.S | re.I)
    sql = (match.group(1).strip() if match else "").strip()
    lower = sql.lower()
    if any(marker in lower for marker in ["allowed columns", "oracle syntax", "question:", "return only oracle sql"]):
        return ""
    return sql


def _intent_to_binds(intent: dict) -> dict:
    binds: dict[str, object] = {}
    if intent.get("has_time_window"):
        today = date.today()
        start = today - timedelta(days=30)
        binds["date_start"] = start
        binds["date_end"] = today
    if intent.get("top_n"):
        binds["top_n"] = int(intent["top_n"])
    return binds


def nl_to_sql_with_llm(question: str, ctx: dict | None = None) -> dict:
    model = get_model("sql")
    if not model:
        raise RuntimeError("SQL model not available")
    intent = clarify_intent(question, ctx or {})

    prompt = _sql_prompt(question, intent)
    raw1 = model.generate(
        prompt,
        max_new_tokens=int(os.getenv("SQL_MAX_NEW_TOKENS", "480")),
        stop=["```"],
    )
    sql1 = _extract_sql_block(raw1)

    from .validator import analyze_sql  # local import to avoid cycles

    validation1 = analyze_sql(sql1)
    if validation1["ok"]:
        return {
            "sql": sql1,
            "used_repair": False,
            "intent": intent,
            "raw": raw1,
            "validation": validation1,
            "binds": _intent_to_binds(intent),
        }

    repair_prompt = _sql_repair_prompt(question, sql1, validation1["errors"])
    raw2 = model.generate(
        repair_prompt,
        max_new_tokens=int(os.getenv("SQL_MAX_NEW_TOKENS", "480")),
        stop=["```"],
    )
    sql2 = _extract_sql_block(raw2)
    validation2 = analyze_sql(sql2)

    if validation2["ok"]:
        return {
            "sql": sql2,
            "used_repair": True,
            "intent": intent,
            "raw": raw2,
            "validation": validation2,
            "binds": _intent_to_binds(intent),
        }

    return {
        "sql": sql2 or sql1 or "",
        "used_repair": True,
        "intent": intent,
        "raw": raw2 or raw1,
        "validation": validation2,
        "binds": _intent_to_binds(intent),
        "first_pass": {
            "raw": raw1,
            "sql": sql1,
            "validation": validation1,
        },
    }

