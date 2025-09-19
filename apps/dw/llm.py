"""DocuWare SQL helpers with temporal intent extraction."""

from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Tuple

from core.model_loader import get_model

ALLOWED_DATE_COLUMNS = ["REQUEST_DATE", "END_DATE"]
ALLOWED_COLUMNS = [
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

_SQL_START_RE = re.compile(r"(?is)\b(with|select)\b")


def clarify_time_intent(question: str) -> Dict[str, Any]:
    """Use the clarifier model to produce structured temporal intent."""

    mdl = get_model("clarifier")
    if mdl is None:
        return {"kind": "NONE", "column": None}

    prompt = f"""
You are a temporal intent extractor for Oracle contracts analytics.

Return ONLY JSON, no prose.

Detect if the question implies any time window. Output:
- kind: "NONE" | "RELATIVE" | "BETWEEN" | "ABSOLUTE"
- column: one of {ALLOWED_DATE_COLUMNS} if clear, else null
- rel: for RELATIVE, one of: "next_7_days","next_30_days","last_7_days","last_30_days","last_month","this_month","last_quarter","this_quarter"
- start, end: ISO dates for BETWEEN or ABSOLUTE windows
- note: short human hint (optional)

Examples:
Q: "Contracts with END_DATE in the next 30 days"
{{"kind":"RELATIVE","column":"END_DATE","rel":"next_30_days"}}

Q: "contracts created last month"
{{"kind":"RELATIVE","column":"REQUEST_DATE","rel":"last_month"}}

Q: "Between 2025-01-01 and 2025-03-31"
{{"kind":"BETWEEN","column":null,"start":"2025-01-01","end":"2025-03-31"}}

Q: "Contracts where VAT is null or zero but net value > 0"
{{"kind":"NONE","column":null}}

Now the question:
{question}
"""

    out = mdl.generate(prompt=prompt.strip(), max_new_tokens=256, temperature=0.0)
    if not out:
        return {"kind": "NONE", "column": None}

    try:
        jstart = out.find("{")
        jend = out.rfind("}") + 1
        parsed = json.loads(out[jstart:jend])
    except Exception:
        return {"kind": "NONE", "column": None}

    kind = (parsed or {}).get("kind")
    if kind not in {"NONE", "RELATIVE", "BETWEEN", "ABSOLUTE"}:
        kind = "NONE"
    column = parsed.get("column")
    if column:
        column = str(column).upper()
        if column not in ALLOWED_DATE_COLUMNS:
            column = None
    intent = {
        "kind": kind,
        "column": column,
    }
    if kind in {"RELATIVE", "BETWEEN", "ABSOLUTE"}:
        if parsed.get("rel"):
            intent["rel"] = str(parsed["rel"]).lower()
        if parsed.get("start"):
            intent["start"] = str(parsed["start"])
        if parsed.get("end"):
            intent["end"] = str(parsed["end"])
        if parsed.get("note"):
            intent["note"] = str(parsed["note"])
    return intent


def _normalize_datetime(value: datetime | None) -> Optional[datetime]:
    if value is None:
        return None
    if value.tzinfo is None:
        return value
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def _parse_iso_date(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if isinstance(parsed, datetime):
        return _normalize_datetime(parsed)
    return None


def derive_window_from_intent(
    intent: Optional[Dict[str, Any]], now: Optional[datetime] = None
) -> Tuple[Dict[str, datetime], Dict[str, Optional[str]]]:
    """Return binds (date_start/date_end) and metadata notes for the supplied intent."""

    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)

    intent = intent or {}
    kind = intent.get("kind", "NONE")
    column = intent.get("column") or None
    notes = {"date_column": column, "window_label": None}
    binds: Dict[str, datetime] = {}

    if kind == "NONE":
        return binds, notes

    if kind in {"BETWEEN", "ABSOLUTE"}:
        start = _parse_iso_date(intent.get("start"))
        end = _parse_iso_date(intent.get("end"))
        if start and end:
            binds["date_start"] = start
            binds["date_end"] = end
            notes["window_label"] = f"{start.date()}..{end.date()}"
        return binds, notes

    if kind == "RELATIVE":
        rel = str(intent.get("rel", "")).lower()
        now_day = now.replace(hour=0, minute=0, second=0, microsecond=0)
        start: Optional[datetime]
        end: Optional[datetime]
        if rel == "next_30_days":
            start = now_day
            end = now_day + timedelta(days=30)
        elif rel == "next_7_days":
            start = now_day
            end = now_day + timedelta(days=7)
        elif rel == "last_30_days":
            end = now_day
            start = now_day - timedelta(days=30)
        elif rel == "last_7_days":
            end = now_day
            start = now_day - timedelta(days=7)
        elif rel == "last_month":
            first_this = now_day.replace(day=1)
            last_month_end = first_this - timedelta(days=1)
            start = last_month_end.replace(day=1)
            end = first_this
        elif rel in {"this_month", "last_quarter", "this_quarter"}:
            end = now_day
            start = now_day - timedelta(days=30)
        else:
            return binds, notes
        binds["date_start"] = _normalize_datetime(start)
        binds["date_end"] = _normalize_datetime(end)
        notes["window_label"] = rel
        return binds, notes

    return binds, notes


def _clean_sql_output(raw: str) -> str:
    if not raw:
        return ""
    text = raw.strip()
    fence = re.search(r"```(sql)?(.*?)```", text, flags=re.IGNORECASE | re.DOTALL)
    if fence:
        text = fence.group(2).strip()
    match = _SQL_START_RE.search(text)
    if match:
        text = text[match.start() :]
    return text.strip().rstrip(";")


def nl_to_sql_with_llm(
    question: str,
    context: Optional[Dict[str, Any]] = None,
    intent: Optional[Dict[str, Any]] = None,
) -> str:
    """Prompt SQLCoder to translate natural language into Oracle SQL."""

    sql_mdl = get_model("sql")
    if sql_mdl is None:
        raise RuntimeError("SQL model unavailable")

    ctx = dict(context or {})
    table_name = ctx.get("contract_table") or "Contract"
    time_block = (intent or {}).get("time") or {}
    time_kind = time_block.get("kind", "NONE")
    date_col = time_block.get("column") or None

    rules = [
        "ONLY Oracle SQL. One SELECT (CTE allowed). No prose.",
        f'Table name is exactly "{table_name}" (quoted).',
        f"Allowed columns only: {', '.join(ALLOWED_COLUMNS)}.",
        "Use Oracle functions such as NVL, LISTAGG ... WITHIN GROUP, TRIM, UPPER, FETCH FIRST N ROWS ONLY.",
        'If computing gross value, use NVL(CONTRACT_VALUE_NET_OF_VAT,0) + NVL(VAT,0) AS CONTRACT_VALUE_GROSS.',
        'Do NOT add date filters unless intent.time.kind != "NONE".',
    ]

    if time_kind != "NONE":
        target_col = date_col or "REQUEST_DATE"
        rules.append(
            f"When a time window is requested, filter on {target_col} using :date_start and :date_end binds."
        )

    prompt_lines = [
        "You are a senior SQL analyst. Follow the rules strictly.",
        "Rules:",
    ]
    for rule in rules:
        prompt_lines.append(f"- {rule}")
    prompt_lines.append("")
    prompt_lines.append(f"Question:\n{question.strip()}")
    if intent:
        prompt_lines.append("")
        prompt_lines.append(f"Intent JSON: {json.dumps(intent, ensure_ascii=False)}")
    prompt_lines.append("")
    prompt_lines.append("SQL:")

    prompt = "\n".join(prompt_lines).strip() + "\n"

    raw_sql = sql_mdl.generate(
        prompt,
        max_new_tokens=512,
        temperature=0.0,
        top_p=0.9,
    )
    return _clean_sql_output(raw_sql)
