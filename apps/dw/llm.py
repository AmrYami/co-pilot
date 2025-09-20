import json, os, re
from typing import Dict, List, Optional, Tuple

from core.model_loader import get_model

STOP_TOKENS = os.environ.get("SQL_STOP", "</s>,<|im_end|").split(",")

CLARIFIER_JSON_MARKER_START = "<<JSON>>"
CLARIFIER_JSON_MARKER_END = "<<END_JSON>>"
SQL_MARKER_START = "<<SQL>>"
SQL_MARKER_END = "<<END_SQL>>"


def _clean(s: str) -> str:
    return (s or "").strip()


def _extract_fenced_sql(text: str) -> Optional[str]:
    """Extract SQL between ```sql ... ``` or our <<SQL>> ... <<END_SQL>> markers."""
    if not text:
        return None
    # Preferred: <<SQL>> ... <<END_SQL>>
    m = re.search(
        re.escape(SQL_MARKER_START) + r"(.*)" + re.escape(SQL_MARKER_END),
        text,
        re.S | re.I,
    )
    if m:
        return m.group(1).strip()
    # Fallback: triple-backtick sql
    m = re.search(r"```(?:sql)?\s*(.*?)```", text, re.S | re.I)
    if m:
        return m.group(1).strip()
    # Fallback: last SELECT/WITH block
    m = re.search(r"((?:SELECT|WITH)\b[\s\S]+)$", text, re.I)
    if m:
        return m.group(1).strip()
    return None


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
    # We prefer literal FETCH FIRST N when top_n_literal provided to avoid driver bind issues.
    top_hint = ""
    if top_n_literal and top_n_literal > 0:
        top_hint = (
            f"\n- If a TOP clause is implied, prefer a literal `FETCH FIRST {top_n_literal} ROWS ONLY`."
        )

    date_hint = f"- Default date column: {default_date_column}."
    if suggested_date_column and suggested_date_column != default_date_column:
        date_hint += f" If a window is requested, prefer {suggested_date_column}."
    needs_window = "Yes" if force_date_binds else "No"

    prompt = f"""Return Oracle SQL only between {SQL_MARKER_START} and {SQL_MARKER_END}. No prose. No comments.
Rules:
- Table: "{table_name}"
- Allowed columns only: {cols}
- Use Oracle syntax: NVL(), TRIM(), UPPER(), LISTAGG(... WITHIN GROUP (...)), FETCH FIRST N ROWS ONLY.
- Do not modify data. SELECT / CTE only.
- Use named binds only from this whitelist when binds are needed: {binds}.{top_hint}
- Do not add any date filter **unless** the user explicitly requests a time window (e.g., "last month", "next 30 days", "in 2024"). 
  When a time window IS requested, use binds :date_start and :date_end with the appropriate date column.
- {date_hint}
- Question implies time window? {needs_window}

Question:
{question}

{SQL_MARKER_START}
"""
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
    top_hint = ""
    if top_n_literal and top_n_literal > 0:
        top_hint = (
            f"\n- If a TOP clause is implied, prefer a literal `FETCH FIRST {top_n_literal} ROWS ONLY`."
        )

    prompt = f"""Previous SQL had validation errors:
{json.dumps(validation_errors)}

Repair the SQL. Return Oracle SQL only between {SQL_MARKER_START} and {SQL_MARKER_END}. No prose. No comments.
Rules:
- Table: "{table_name}"
- Allowed columns only: {cols}
- Use Oracle syntax: NVL(), TRIM(), UPPER(), LISTAGG(... WITHIN GROUP (...)), FETCH FIRST N ROWS ONLY.
- Use only whitelisted binds: {binds}.{top_hint}
- When a time window is requested, use :date_start and :date_end on the correct date column.
- Default date column: {default_date_column}.
"""
    if suggested_date_column and suggested_date_column != default_date_column:
        prompt += (
            f"- Prefer {suggested_date_column} for the time window when explicitly requested.\n"
        )

    prompt += f"""
Question:
{question}

Previous SQL to repair:
```sql
{prev_sql}
```

{SQL_MARKER_START}
"""
    return prompt


def nl_to_sql_raw(prompt: str) -> str:
    mdl = get_model("sql")
    # Keep flags minimal; your loader already warns about unsupported ones.
    return mdl.generate(prompt, stop=STOP_TOKENS)


def extract_sql(generated_text: str) -> Optional[str]:
    sql = _extract_fenced_sql(generated_text)
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
    "extract_sql",
    "clarify_intent",
]
