import json
import re

from flask import current_app

from core.model_loader import get_model

SQL_START_RE = re.compile(r"(?is)\b(select|with)\b")


def _extract_sql(text: str) -> str:
    """Return the SQL segment starting from the first SELECT/WITH token."""

    if not text:
        return ""
    match = SQL_START_RE.search(text)
    if not match:
        return ""
    return text[match.start():].strip()


def clarify_intent(question: str, context: dict | None = None) -> dict:
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
    payload = {}
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
    logger = current_app.logger if current_app else None
    if logger:
        logger.info("[dw] clarifier_raw", extra={"json": {
            "raw": text[:2000],
            "intent": intent,
        }})
    return intent


def _build_sql_prompt(question: str, context: dict, intent: dict) -> str:
    """Construct a compact, fence-free SQL prompt."""

    table = context.get("contract_table") or "Contract"
    allowed_cols = (
        "CONTRACT_ID, CONTRACT_OWNER, "
        "CONTRACT_STAKEHOLDER_1, CONTRACT_STAKEHOLDER_2, CONTRACT_STAKEHOLDER_3, CONTRACT_STAKEHOLDER_4, "
        "CONTRACT_STAKEHOLDER_5, CONTRACT_STAKEHOLDER_6, CONTRACT_STAKEHOLDER_7, CONTRACT_STAKEHOLDER_8, "
        "DEPARTMENT_1, DEPARTMENT_2, DEPARTMENT_3, DEPARTMENT_4, DEPARTMENT_5, DEPARTMENT_6, DEPARTMENT_7, DEPARTMENT_8, "
        "OWNER_DEPARTMENT, CONTRACT_VALUE_NET_OF_VAT, VAT, CONTRACT_PURPOSE, CONTRACT_SUBJECT, "
        "START_DATE, END_DATE, REQUEST_DATE, REQUEST_TYPE, CONTRACT_STATUS, ENTITY_NO, REQUESTER"
    )
    shot1_q = "Contracts where VAT is null or zero but CONTRACT_VALUE_NET_OF_VAT > 0."
    shot1_a = (
        f"SELECT CONTRACT_ID, CONTRACT_OWNER, OWNER_DEPARTMENT, REQUEST_DATE AS REF_DATE, CONTRACT_STATUS,\n"
        f"       NVL(CONTRACT_VALUE_NET_OF_VAT,0) AS NET_VALUE, NVL(VAT,0) AS VAT,\n"
        f"       NVL(CONTRACT_VALUE_NET_OF_VAT,0) + NVL(VAT,0) AS CONTRACT_VALUE_GROSS\n"
        f"  FROM \"{table}\"\n"
        f" WHERE NVL(VAT,0) = 0 AND NVL(CONTRACT_VALUE_NET_OF_VAT,0) > 0\n"
        f" ORDER BY REQUEST_DATE DESC"
    )
    time_hint = ""
    if intent.get("has_time_window"):
        col = intent.get("date_column") or context.get("default_date_col") or "REQUEST_DATE"
        time_hint = f"\nWhen a time window is requested, filter {col} between :date_start and :date_end."
    prompt = (
        "Generate Oracle SQL only. Output must begin with SELECT or WITH. No comments. No prose.\n"
        f"Use only table \"{table}\" and only these columns: {allowed_cols}.\n"
        "Use Oracle syntax (NVL, TRIM, LISTAGG WITHIN GROUP, FETCH FIRST N ROWS ONLY). "
        "Use named binds only from this whitelist when needed: :date_start, :date_end, :top_n, :owner_name, :dept, :entity_no, :contract_id_pattern, :request_type."
        f"{time_hint}\n"
        f"\nExample:\nQ: {shot1_q}\nA:\n{shot1_a}\n"
        f"\nQ: {question}\nA:\n"
    )
    return prompt


def nl_to_sql_with_llm(question: str, context: dict) -> dict:
    """Clarify intent, run the SQL model, and return intermediate artifacts."""

    intent = clarify_intent(question, context)
    sql_model = get_model("sql")
    prompt = _build_sql_prompt(question, context, intent)
    logger = current_app.logger if current_app else None
    if logger:
        logger.info("[dw] sql_prompt", extra={"json": {"prompt": prompt[:2000]}})
    raw1 = sql_model.generate(prompt, max_new_tokens=384, stop=[])
    if logger:
        logger.info("[dw] llm_raw_pass1", extra={"json": {"text": raw1[:1200]}})
    sql1 = _extract_sql(raw1)
    if logger:
        logger.info("[dw] llm_sql_pass1", extra={"json": {"sql": sql1[:1200]}})
    return {
        "ok": True,
        "intent": intent,
        "prompt": prompt,
        "raw1": raw1,
        "sql1": sql1,
        "used_repair": False,
    }


__all__ = ["clarify_intent", "nl_to_sql_with_llm", "_extract_sql"]
