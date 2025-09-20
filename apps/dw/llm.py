from __future__ import annotations

import json
import re
from typing import Any, Dict, List

from flask import current_app

from core.model_loader import get_model

ALLOWED_COLS = (
    "CONTRACT_ID, CONTRACT_OWNER, "
    "CONTRACT_STAKEHOLDER_1, CONTRACT_STAKEHOLDER_2, CONTRACT_STAKEHOLDER_3, CONTRACT_STAKEHOLDER_4, "
    "CONTRACT_STAKEHOLDER_5, CONTRACT_STAKEHOLDER_6, CONTRACT_STAKEHOLDER_7, CONTRACT_STAKEHOLDER_8, "
    "DEPARTMENT_1, DEPARTMENT_2, DEPARTMENT_3, DEPARTMENT_4, DEPARTMENT_5, DEPARTMENT_6, DEPARTMENT_7, DEPARTMENT_8, "
    "OWNER_DEPARTMENT, CONTRACT_VALUE_NET_OF_VAT, VAT, CONTRACT_PURPOSE, CONTRACT_SUBJECT, "
    "START_DATE, END_DATE, REQUEST_DATE, REQUEST_TYPE, CONTRACT_STATUS, ENTITY_NO, REQUESTER"
)

BIND_WHITELIST = {
    "date_start",
    "date_end",
    "top_n",
    "owner_name",
    "dept",
    "entity_no",
    "contract_id_pattern",
    "request_type",
}


def build_sql_prompt(
    question: str,
    intent: Dict[str, Any],
    *,
    table: str = "Contract",
    default_date_col: str = "REQUEST_DATE",
) -> str:
    """Construct the fence-free SQL generation prompt."""

    date_col = intent.get("date_column") or default_date_col
    top_n = intent.get("top_n")
    wants_top = f"yes, {top_n}" if top_n else "no"

    rules: List[str] = [
        "Return ONLY Oracle SQL.",
        'Output MUST start with "SELECT" or "WITH".',
        f'Use only table "{table}".',
        f"Allowed columns only: {ALLOWED_COLS}",
        "Use Oracle syntax: NVL(), TRIM(), UPPER(), LISTAGG(... WITHIN GROUP (...)), FETCH FIRST N ROWS ONLY.",
        "Do not modify data. SELECT / CTE only.",
        f"Use named binds only from this whitelist when needed: {', '.join(sorted(BIND_WHITELIST))}.",
        "Do not add any date filter unless the user explicitly requests a time window.",
        "When a time window IS requested, use :date_start and :date_end on the appropriate date column.",
        f"Default date column for windows: {date_col}.",
        f"Top-N implied? {wants_top}.",
        "Start now. First token must be SELECT or WITH.",
    ]
    return "\n".join(rules) + f"\n\nQuestion:\n{question}\n\nSQL:\n"


def build_repair_prompt(
    question: str,
    bad_sql: str,
    errors: List[str],
    intent: Dict[str, Any],
    *,
    table: str = "Contract",
    default_date_col: str = "REQUEST_DATE",
) -> str:
    """Prompt to repair invalid SQL from the first pass."""

    rules: List[str] = [
        f"Previous SQL had validation errors: {errors}",
        "Repair it. Return ONLY Oracle SQL.",
        'Output MUST start with "SELECT" or "WITH".',
        f'Use only table "{table}".',
        f"Allowed columns only: {ALLOWED_COLS}",
        "Use Oracle syntax: NVL(), TRIM(), UPPER(), LISTAGG(... WITHIN GROUP (...)), FETCH FIRST N ROWS ONLY.",
        f"Use ONLY whitelisted binds: {', '.join(sorted(BIND_WHITELIST))}.",
        "If a time window is requested, use :date_start and :date_end on the correct date column.",
        f"Default date column for windows: {intent.get('date_column') or default_date_col}.",
        "Start now. First token must be SELECT or WITH.",
    ]
    return (
        "\n".join(rules)
        + f"\n\nQuestion:\n{question}\n\nPrevious SQL:\n{bad_sql}\n\nSQL:\n"
    )


def call_sql_model(prompt: str, *, max_new_tokens: int = 256) -> str:
    """Invoke the SQL model with the provided prompt."""

    mdl = get_model("sql")
    return mdl.generate(prompt, max_new_tokens=max_new_tokens) or ""


def extract_sql(text: str) -> str:
    """Extract the first SELECT/CTE block from the model output."""

    if not text:
        return ""

    trimmed = text.strip()
    match = re.search(r"(?is)\b(SELECT|WITH)\b", trimmed)
    if not match:
        return ""

    sql = trimmed[match.start():].strip()
    semi = sql.find(";")
    if semi != -1:
        sql = sql[:semi].strip()

    if not re.match(r"(?is)^(SELECT|WITH)\b", sql):
        return ""

    return sql


def clarify_intent(question: str, context: dict | None = None) -> Dict[str, Any]:
    """Run the clarifier model and return a normalized intent dictionary."""

    mdl = get_model("clarifier")
    prompt = (
        "You are a precise NLU clarifier. Analyze the user's question and output JSON only.\n"
        "Extract keys exactly as follows:\n"
        "  has_time_window: boolean\n"
        "  date_column: string|null (one of END_DATE, REQUEST_DATE, START_DATE)\n"
        "  top_n: integer|null\n"
        "  explicit_dates: object|null with keys {start: ISO-8601 date, end: ISO-8601 date}\n"
        "Return JSON only between <<JSON>> and <</JSON>>.\n\n"
        f"Question: {question}\n\n<<JSON>>\n{{}}\n<</JSON>>\n"
    )
    raw = mdl.generate(prompt, max_new_tokens=192)
    text = raw if isinstance(raw, str) else str(raw)
    start = text.find("<<JSON>>")
    end = text.find("<</JSON>>")
    payload: Dict[str, Any] = {}
    if 0 <= start < end:
        body = text[start + 8 : end].strip()
        if body:
            try:
                payload = json.loads(body)
            except Exception:
                payload = {}
    intent = {
        "has_time_window": bool(payload.get("has_time_window")),
        "date_column": payload.get("date_column"),
        "top_n": payload.get("top_n"),
        "explicit_dates": payload.get("explicit_dates"),
    }
    if current_app:
        try:
            current_app.logger.info(
                "[dw] clarifier_raw", extra={"json": {"raw": text[:2000], "intent": intent}}
            )
        except Exception:
            current_app.logger.info(f"[dw] clarifier_raw: {intent}")
    return intent


def nl_to_sql_with_llm(question: str, context: dict) -> Dict[str, Any]:
    """Clarify intent, run the SQL model, and return intermediate artifacts."""

    intent = clarify_intent(question, context)
    table = context.get("contract_table") or "Contract"
    default_date_col = context.get("default_date_col") or "REQUEST_DATE"

    prompt = build_sql_prompt(
        question,
        intent,
        table=table,
        default_date_col=default_date_col,
    )
    if current_app:
        try:
            current_app.logger.info("[dw] sql_prompt", extra={"json": {"prompt": prompt[:2000]}})
        except Exception:
            current_app.logger.info(f"[dw] sql_prompt: {prompt[:500]}")

    raw1 = call_sql_model(prompt, max_new_tokens=384)
    if current_app:
        try:
            current_app.logger.info("[dw] llm_raw_pass1", extra={"json": {"text": raw1[:1200]}})
        except Exception:
            current_app.logger.info(f"[dw] llm_raw_pass1: {raw1[:500]}")

    sql1 = extract_sql(raw1)
    if current_app:
        try:
            current_app.logger.info("[dw] llm_sql_pass1", extra={"json": {"sql": sql1[:1200]}})
        except Exception:
            current_app.logger.info(f"[dw] llm_sql_pass1: {sql1[:500]}")

    return {
        "ok": True,
        "intent": intent,
        "prompt": prompt,
        "raw1": raw1,
        "sql1": sql1,
        "used_repair": False,
    }


_extract_sql = extract_sql


__all__ = [
    "ALLOWED_COLS",
    "BIND_WHITELIST",
    "build_repair_prompt",
    "build_sql_prompt",
    "call_sql_model",
    "clarify_intent",
    "extract_sql",
    "nl_to_sql_with_llm",
]
