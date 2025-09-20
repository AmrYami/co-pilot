import json, os, re
from typing import List, Optional

from core.model_loader import get_model

STOP_TOKENS = os.environ.get("SQL_STOP", "</s>,<|im_end|").split(",")
if "```" not in STOP_TOKENS:
    STOP_TOKENS.append("```")

CLARIFIER_JSON_MARKER_START = "<<JSON>>"
CLARIFIER_JSON_MARKER_END = "<<END_JSON>>"

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


def clarify_intent(question: str) -> dict:
    """
    Ask the clarifier to return structured JSON inside <<JSON>> ... <<END_JSON>>:
    { "has_time_window": bool,
      "date_column": "REQUEST_DATE" | "END_DATE" | null,
      "top_n": int | null,
      "explicit_dates": {"date_start":"YYYY-MM-DD","date_end":"YYYY-MM-DD"} | null
    }
    """
    try:
        mdl = get_model("clarifier")
    except Exception as e:
        return {"ok": False, "used": False, "raw": None, "error": str(e)}

    prompt = f"""You are a precise NLU clarifier. Analyze the user's question and output JSON only.
Extract:
- has_time_window (bool): whether the question requests a time window (e.g., last month, next 30 days, in 2024, between ...).
- date_column (string|null): which date field to use if implied or stated explicitly (END_DATE, REQUEST_DATE, START_DATE). null if unspecified.
- top_n (int|null): number for "top N" requests; null if not requested.
- explicit_dates (object|null): ISO dates if explicit like "between 2024-01-01 and 2024-03-01".

Return JSON only between {CLARIFIER_JSON_MARKER_START} and {CLARIFIER_JSON_MARKER_END}.

Question:
{question}

{CLARIFIER_JSON_MARKER_START}
{{}}
{CLARIFIER_JSON_MARKER_END}
"""
    raw = mdl.generate(prompt, stop=[CLARIFIER_JSON_MARKER_END])
    # Extract between markers
    m = re.search(re.escape(CLARIFIER_JSON_MARKER_START) + r"(.*)", raw, re.S)
    intent = {}
    if m:
        payload = m.group(1).strip()
        # Cut trailing marker if present
        payload = payload.split(CLARIFIER_JSON_MARKER_END)[0].strip()
        try:
            intent = json.loads(payload)
        except Exception:
            intent = {}
    return {"ok": True, "used": True, "raw": raw, "intent": intent}


__all__ = [
    "build_sql_prompt",
    "build_sql_repair_prompt",
    "nl_to_sql_raw",
    "extract_sql_only",
    "extract_sql",
    "clarify_intent",
]
