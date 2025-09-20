import json, os, re, logging
from typing import List, Optional

from flask import current_app

from core.model_loader import get_model
from core.logging_setup import log_kv

STOP_TOKENS = os.environ.get("SQL_STOP", "</s>,<|im_end|").split(",")
if "```" not in STOP_TOKENS:
    STOP_TOKENS.append("```")

CLARIFIER_JSON_MARKER_START = "<<JSON>>"
CLARIFIER_JSON_MARKER_END = "<</JSON>>"

_FENCE_RE = re.compile(r"```(?:sql)?\s*(.*?)```", re.IGNORECASE | re.DOTALL)
_TAG_RE = re.compile(r"<SQL>(.*?)</SQL>", re.IGNORECASE | re.DOTALL)
_HEAD_RE = re.compile(r"(?is)\b(SELECT|WITH)\b")


def extract_sql_only(text: str) -> str:
    if not text:
        return ""
    fences = _FENCE_RE.findall(text)
    for chunk in reversed(fences):
        candidate = chunk.strip()
        if candidate:
            return candidate
    tags = _TAG_RE.findall(text)
    for chunk in reversed(tags):
        candidate = chunk.strip()
        if candidate:
            return candidate
    m = _HEAD_RE.search(text)
    if m:
        return text[m.start():].strip()
    return ""


def build_sql_prompt(
    question: str,
    *,
    table_name: str,
    allowed_columns: List[str],
    allowed_binds: List[str],
    default_date_column: str,
    force_date_binds: bool,
    suggested_date_column: Optional[str],
    top_n_literal: Optional[int] = None,
) -> str:
    """
    A short deterministic prompt tailored for SQLCoder:
    - Never allow prose in output
    - Only allowed columns
    - Only whitelisted binds when needed
    - Oracle dialect hints
    """
    cols = ", ".join(allowed_columns)
    binds = ", ".join(allowed_binds)
    date_hint = f"Default date column: {default_date_column}."
    if suggested_date_column and suggested_date_column != default_date_column:
        date_hint += f" If a window is requested, prefer {suggested_date_column}."
    needs_window = "Yes" if force_date_binds else "No"

    window_anchor = suggested_date_column or default_date_column
    rules = [
        f'Use only table "{table_name}".',
        f"Allowed columns only: {cols}",
        "Use Oracle syntax: NVL(), TRIM(), UPPER(), LISTAGG(... WITHIN GROUP (...)), FETCH FIRST N ROWS ONLY.",
        "Do not modify data. SELECT / CTE only.",
        f"Use named binds only from this whitelist when binds are needed: {binds}.",
        "Do not add any date filter unless the user explicitly requests a time window (e.g., last month, next 30 days, in 2024).",
        "When a time window IS requested, use binds :date_start and :date_end.",
        f"If the user asks for a time window but does not name a date column, use {window_anchor} for the filter.",
        f"Question implies time window? {needs_window}",
        date_hint,
        "Close the fenced block after the SQL with ```.",
    ]
    if top_n_literal and top_n_literal > 0:
        rules.append(
            f"If a TOP clause is implied, prefer a literal `FETCH FIRST {top_n_literal} ROWS ONLY`."
        )
    prompt = (
        "Return only Oracle SQL inside a fenced block."
        "\n```sql\n"
        "-- your SQL here\n"
        "```\n"
        + "\n".join(f"- {rule}" for rule in rules)
        + f"\n\nQuestion:\n{question}\n\nAnswer with:\n```sql\n"
    )
    return prompt


def build_sql_repair_prompt(
    question: str,
    prev_sql: str,
    validation_errors: List[str],
    *,
    table_name: str,
    allowed_columns: List[str],
    allowed_binds: List[str],
    default_date_column: str,
    suggested_date_column: Optional[str],
    top_n_literal: Optional[int] = None,
) -> str:
    cols = ", ".join(allowed_columns)
    binds = ", ".join(allowed_binds)

    prompt = f"""Previous SQL had validation errors:
{json.dumps(validation_errors)}

Repair the SQL. Return Oracle SQL only inside a fenced block. No prose. No comments.
Rules:
- Table: "{table_name}"
- Allowed columns only: {cols}
- Use Oracle syntax: NVL(), TRIM(), UPPER(), LISTAGG(... WITHIN GROUP (...)), FETCH FIRST N ROWS ONLY.
- Use only whitelisted binds: {binds}.
- When a time window is requested, use :date_start and :date_end on the correct date column.
- Default date column: {default_date_column}.
"""
    if suggested_date_column and suggested_date_column != default_date_column:
        prompt += (
            f"- Prefer {suggested_date_column} for the time window when explicitly requested.\n"
        )
    if top_n_literal and top_n_literal > 0:
        prompt += (
            f"- If a TOP clause is implied, prefer a literal `FETCH FIRST {top_n_literal} ROWS ONLY`.\n"
        )

    prompt += f"""
Question:
{question}

Previous SQL to repair:
```sql
{prev_sql}
```

Answer with:
```sql
"""
    return prompt


def nl_to_sql_raw(prompt: str) -> str:
    mdl = get_model("sql")
    # Keep flags minimal; your loader already warns about unsupported ones.
    return mdl.generate(prompt, stop=STOP_TOKENS)


def extract_sql(generated_text: str) -> Optional[str]:
    sql = extract_sql_only(generated_text)
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
    "extract_sql_only",
    "extract_sql",
    "clarify_intent",
]
