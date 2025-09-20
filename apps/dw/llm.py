import json, os, re, logging
from typing import List, Optional

from flask import current_app

from core.model_loader import get_model
from core.logging_setup import log_kv

STOP_TOKENS = [tok for tok in os.environ.get("SQL_STOP", "</s>,<|im_end|").split(",") if tok]
STOP_TOKENS = [tok for tok in STOP_TOKENS if "```" not in tok]

CLARIFIER_JSON_MARKER_START = "<<JSON>>"
CLARIFIER_JSON_MARKER_END = "<</JSON>>"

_FENCE_RE = re.compile(r"```(?:sql)?\s*(.+?)\s*```", re.IGNORECASE | re.DOTALL)
_SQL_START_RE = re.compile(r"\b(WITH\b.*|SELECT\b.*)$", re.IGNORECASE | re.DOTALL)


def _extract_sql_candidate(text: str) -> str:
    if not text:
        return ""
    fenced = _FENCE_RE.search(text)
    if fenced:
        return fenced.group(1).strip()
    unfenced = _SQL_START_RE.search(text)
    if unfenced:
        return unfenced.group(1).strip()
    return text.strip()


def build_sql_prompt(
    question: str,
    *,
    table_name: str,
    allowed_columns: List[str],
    allow_binds: List[str],
    time_window_hint: Optional[dict],
    top_n_literal: Optional[int] = None,
) -> str:
    """Construct the base SQL prompt shown to SQLCoder."""

    fewshot = (
        "Example:\n"
        "User: top 5 stakeholders by gross value last month\n"
        "Assistant (SQL):\n"
        "WITH stakeholders AS (\n"
        "  SELECT CONTRACT_ID,\n"
        "         NVL(CONTRACT_VALUE_NET_OF_VAT,0)+NVL(VAT,0) AS CONTRACT_VALUE_GROSS,\n"
        "         CONTRACT_STAKEHOLDER_1 AS STAKEHOLDER,\n"
        "         DEPARTMENT_1 AS DEPARTMENT,\n"
        "         REQUEST_DATE AS REF_DATE\n"
        "    FROM \"Contract\"\n"
        "  UNION ALL SELECT CONTRACT_ID, NVL(CONTRACT_VALUE_NET_OF_VAT,0)+NVL(VAT,0), CONTRACT_STAKEHOLDER_2, DEPARTMENT_2, REQUEST_DATE FROM \"Contract\"\n"
        "  UNION ALL SELECT CONTRACT_ID, NVL(CONTRACT_VALUE_NET_OF_VAT,0)+NVL(VAT,0), CONTRACT_STAKEHOLDER_3, DEPARTMENT_3, REQUEST_DATE FROM \"Contract\"\n"
        "  UNION ALL SELECT CONTRACT_ID, NVL(CONTRACT_VALUE_NET_OF_VAT,0)+NVL(VAT,0), CONTRACT_STAKEHOLDER_4, DEPARTMENT_4, REQUEST_DATE FROM \"Contract\"\n"
        "  UNION ALL SELECT CONTRACT_ID, NVL(CONTRACT_VALUE_NET_OF_VAT,0)+NVL(VAT,0), CONTRACT_STAKEHOLDER_5, DEPARTMENT_5, REQUEST_DATE FROM \"Contract\"\n"
        "  UNION ALL SELECT CONTRACT_ID, NVL(CONTRACT_VALUE_NET_OF_VAT,0)+NVL(VAT,0), CONTRACT_STAKEHOLDER_6, DEPARTMENT_6, REQUEST_DATE FROM \"Contract\"\n"
        "  UNION ALL SELECT CONTRACT_ID, NVL(CONTRACT_VALUE_NET_OF_VAT,0)+NVL(VAT,0), CONTRACT_STAKEHOLDER_7, DEPARTMENT_7, REQUEST_DATE FROM \"Contract\"\n"
        "  UNION ALL SELECT CONTRACT_ID, NVL(CONTRACT_VALUE_NET_OF_VAT,0)+NVL(VAT,0), CONTRACT_STAKEHOLDER_8, DEPARTMENT_8, REQUEST_DATE FROM \"Contract\"\n"
        ")\n"
        "SELECT TRIM(STAKEHOLDER) AS stakeholder,\n"
        "       SUM(CONTRACT_VALUE_GROSS) AS total_gross_value,\n"
        "       COUNT(DISTINCT CONTRACT_ID) AS contract_count,\n"
        "       LISTAGG(DISTINCT TRIM(DEPARTMENT), ', ') WITHIN GROUP (ORDER BY TRIM(DEPARTMENT)) AS departments\n"
        "  FROM stakeholders\n"
        " WHERE STAKEHOLDER IS NOT NULL AND TRIM(STAKEHOLDER) <> ''\n"
        "   AND REF_DATE >= :date_start AND REF_DATE < :date_end\n"
        " GROUP BY TRIM(STAKEHOLDER)\n"
        " ORDER BY total_gross_value DESC\n"
        " FETCH FIRST 5 ROWS ONLY\n"
    )

    allowed_cols = ", ".join(allowed_columns)
    bind_list = ", ".join(allow_binds)
    top_clause_rule = (
        "If a TOP clause is implied (e.g., top 10), emit a literal FETCH FIRST N ROWS ONLY."
    )
    if top_n_literal and top_n_literal > 0:
        top_clause_rule = (
            f"If a TOP clause is implied, emit a literal FETCH FIRST {top_n_literal} ROWS ONLY."
        )

    instr = (
        "Return Oracle SQL (SELECT or WITH ... SELECT) only.\n"
        "No prose. No comments. No code fences.\n"
        f"Use only table \"{table_name}\".\n"
        f"Allowed columns only: {allowed_cols}\n"
        "Use Oracle syntax: NVL(), TRIM(), UPPER(), LISTAGG(... WITHIN GROUP (...)), FETCH FIRST N ROWS ONLY.\n"
        f"Use named binds only from this whitelist when needed: {bind_list}.\n"
        "Do NOT add date filters unless the user explicitly asks (e.g., last month, next 30 days, in 2024).\n"
        "When a time window IS requested and no date column is named, use REQUEST_DATE with :date_start and :date_end.\n"
        f"{top_clause_rule}\n"
    )

    hint = ""
    if time_window_hint and time_window_hint.get("has_time_window"):
        hinted_col = time_window_hint.get("date_column") or "REQUEST_DATE"
        hint = (
            f"\nHint: apply time window on {hinted_col} using :date_start and :date_end.\n"
        )

    return f"{instr}\n{fewshot}\n\nUser:\n{question}\n\nAssistant (SQL):{hint}\n"


def build_sql_repair_prompt(
    question: str,
    prev_sql: str,
    validation_errors: List[str],
    *,
    table_name: str,
    allowed_columns: List[str],
    allow_binds: List[str],
    time_window_hint: Optional[dict],
    top_n_literal: Optional[int] = None,
) -> str:
    cols = ", ".join(allowed_columns)
    binds = ", ".join(allow_binds)

    top_clause_rule = (
        "- If a TOP clause is implied (e.g., top 10), emit a literal FETCH FIRST N ROWS ONLY.\n"
    )
    if top_n_literal and top_n_literal > 0:
        top_clause_rule = (
            f"- If a TOP clause is implied, emit a literal FETCH FIRST {top_n_literal} ROWS ONLY.\n"
        )

    prompt = (
        f"Previous SQL had validation errors:\n{json.dumps(validation_errors)}\n\n"
        "Repair the SQL. Return Oracle SQL (SELECT or WITH ... SELECT) only.\n"
        "No prose. No comments. No code fences.\n"
        "Rules:\n"
        f"- Table: \"{table_name}\"\n"
        f"- Allowed columns only: {cols}\n"
        "- Use Oracle syntax: NVL(), TRIM(), UPPER(), LISTAGG(... WITHIN GROUP (...)), FETCH FIRST N ROWS ONLY.\n"
        f"- Use only whitelisted binds: {binds}.\n"
        "- Do NOT add date filters unless the user explicitly asks.\n"
        "- When a time window IS requested and no date column is named, use REQUEST_DATE with :date_start and :date_end.\n"
        f"{top_clause_rule}"
    )

    if time_window_hint and time_window_hint.get("has_time_window"):
        hinted_col = time_window_hint.get("date_column") or "REQUEST_DATE"
        prompt += f"- Apply the requested time window on {hinted_col} using :date_start and :date_end.\n"

    prompt += (
        "\nQuestion:\n"
        f"{question}\n\n"
        "Previous SQL to repair:\n"
        f"{prev_sql}\n\n"
        "Assistant (SQL):\n"
    )

    return prompt


def nl_to_sql_raw(prompt: str) -> str:
    mdl = get_model("sql")
    # Keep flags minimal; your loader already warns about unsupported ones.
    return mdl.generate(prompt, stop=STOP_TOKENS)


def extract_sql(generated_text: str) -> Optional[str]:
    sql = _extract_sql_candidate(generated_text)
    if not sql:
        return None
    # Remove accidental comments that some models still insert:
    lines = []
    for ln in sql.splitlines():
        if ln.strip().startswith("--"):
            continue
        lines.append(ln)
    cleaned = "\n".join(lines).strip().rstrip(";")
    return cleaned if cleaned else None


def clarify_intent(question: str, context: Optional[dict] = None) -> dict:
    """Call the clarifier model to extract structured hints about the question."""
    try:
        mdl = get_model("clarifier")
    except Exception as exc:
        return {"ok": False, "used": False, "raw": None, "error": str(exc)}

    context = context or {}
    system_prompt = (
        "You are a precise NLU clarifier. Analyze the user's question and output JSON only.\n"
        "Extract keys exactly as follows:\n"
        "  has_time_window: boolean\n"
        "  date_column: string|null (one of END_DATE, REQUEST_DATE, START_DATE)\n"
        "  top_n: integer|null\n"
        "  explicit_dates: object|null with keys {start: ISO-8601 date, end: ISO-8601 date}\n"
        f"Return JSON only between {CLARIFIER_JSON_MARKER_START} and {CLARIFIER_JSON_MARKER_END}.\n"
    )
    prompt = (
        f"{system_prompt}\n"
        f"Question: {question}\n\n"
        f"{CLARIFIER_JSON_MARKER_START}\n{{}}\n{CLARIFIER_JSON_MARKER_END}\n"
    )

    raw = mdl.generate(prompt, max_new_tokens=256)
    text = raw if isinstance(raw, str) else str(raw)

    pattern = re.escape(CLARIFIER_JSON_MARKER_START) + r"(.*?)" + re.escape(CLARIFIER_JSON_MARKER_END)
    match = re.search(pattern, text, re.S)
    intent: dict = {}
    if match:
        payload = match.group(1).strip()
        try:
            intent = json.loads(payload)
        except Exception:
            intent = {}

    if not intent:
        # Heuristic fallback in case the model returns malformed JSON
        ql = (question or "").lower()
        has_window = any(
            kw in ql
            for kw in [
                "last month",
                "next 30",
                "last 30",
                "last 90",
                "between",
                "in 20",
                "since",
            ]
        )
        date_col = None
        if "end date" in ql or "expiry" in ql or "expires" in ql:
            date_col = "END_DATE"
        elif "start date" in ql:
            date_col = "START_DATE"
        elif "request date" in ql:
            date_col = "REQUEST_DATE"
        top_n = None
        m_top = re.search(r"\btop\s+(\d+)\b", ql)
        if m_top:
            try:
                top_n = int(m_top.group(1))
            except Exception:
                top_n = None
        explicit_dates = None
        m_between = re.search(
            r"between\s+(\d{4}-\d{2}-\d{2})\s+and\s+(\d{4}-\d{2}-\d{2})",
            ql,
        )
        if m_between:
            explicit_dates = {"start": m_between.group(1), "end": m_between.group(2)}
        intent = {
            "has_time_window": has_window,
            "date_column": date_col,
            "top_n": top_n,
            "explicit_dates": explicit_dates,
        }

    logger = current_app.logger if current_app else logging.getLogger(__name__)
    log_kv(
        logger,
        "[clarifier]",
        {
            "prompt_tail": prompt[-800:],
            "raw_head": text[:800],
            "intent": intent,
            "context": context,
        },
    )

    return {"ok": True, "used": True, "raw": text, "intent": intent}


__all__ = [
    "build_sql_prompt",
    "build_sql_repair_prompt",
    "nl_to_sql_raw",
    "extract_sql",
    "clarify_intent",
]
