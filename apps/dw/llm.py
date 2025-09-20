import json
import os
import re
from datetime import datetime, timedelta

from core.model_loader import get_model

# ------------- Clarifier -------------

_JSON_RE = re.compile(r"<<JSON>>\s*(\{.*?\})\s*<</JSON>>", re.S)


def _heuristic_intent(question: str) -> dict:
    q = (question or "").lower()
    has_window = any(w in q for w in ["last month", "next 30 days", "in 2024", "between", "since "])
    date_col = "END_DATE" if any(w in q for w in ["expire", "expiry", "end date"]) else "REQUEST_DATE"
    top_n = None
    m = re.search(r"\btop\s+(\d+)\b", q)
    if m:
        top_n = int(m.group(1))
    return {
        "has_time_window": bool(has_window),
        "date_column": date_col if has_window or "end date" in q or "request date" in q or "start date" in q else None,
        "top_n": top_n,
        "explicit_dates": None,
    }


def clarify_intent(question: str, ctx: dict) -> dict:
    """Return {'intent': {...}, 'ok': True, 'used': bool, 'raw': str}."""

    mdl = get_model("clarifier")
    prompt = (
        "You are a precise NLU clarifier. Output JSON only.\n"
        "Keys:\n"
        "  has_time_window: boolean\n"
        "  date_column: string|null (END_DATE|REQUEST_DATE|START_DATE)\n"
        "  top_n: integer|null\n"
        "  explicit_dates: object|null {start,end} (ISO dates)\n"
        "Return JSON only between <<JSON>> and <</JSON>>.\n\n"
        f"Question: {question}\n\n"
        "<<JSON>>\n{}\n<</JSON>>"
    )

    if not mdl:
        return {"intent": _heuristic_intent(question), "ok": True, "used": False, "raw": ""}

    raw = mdl.generate(prompt, max_new_tokens=192, temperature=0.0)
    m = _JSON_RE.search(raw or "")
    if not m:
        return {"intent": _heuristic_intent(question), "ok": True, "used": False, "raw": raw or ""}

    try:
        data = json.loads(m.group(1))
        filled = _heuristic_intent(question)
        intent = {
            "has_time_window": data.get("has_time_window", filled["has_time_window"]),
            "date_column": data.get("date_column") or filled["date_column"],
            "top_n": data.get("top_n") if isinstance(data.get("top_n"), int) else filled["top_n"],
            "explicit_dates": data.get("explicit_dates") or None,
        }
        return {"intent": intent, "ok": True, "used": True, "raw": raw}
    except Exception:
        return {"intent": _heuristic_intent(question), "ok": True, "used": False, "raw": raw}


# ------------- SQL Prompt -------------

_ALLOWED_COLS = (
    "CONTRACT_ID, CONTRACT_OWNER, "
    "CONTRACT_STAKEHOLDER_1, CONTRACT_STAKEHOLDER_2, CONTRACT_STAKEHOLDER_3, CONTRACT_STAKEHOLDER_4, "
    "CONTRACT_STAKEHOLDER_5, CONTRACT_STAKEHOLDER_6, CONTRACT_STAKEHOLDER_7, CONTRACT_STAKEHOLDER_8, "
    "DEPARTMENT_1, DEPARTMENT_2, DEPARTMENT_3, DEPARTMENT_4, DEPARTMENT_5, DEPARTMENT_6, DEPARTMENT_7, DEPARTMENT_8, "
    "OWNER_DEPARTMENT, CONTRACT_VALUE_NET_OF_VAT, VAT, CONTRACT_PURPOSE, CONTRACT_SUBJECT, "
    "START_DATE, END_DATE, REQUEST_DATE, REQUEST_TYPE, CONTRACT_STATUS, ENTITY_NO, REQUESTER"
)

_WHITELIST_BINDS = "contract_id_pattern, date_end, date_start, dept, entity_no, owner_name, request_type, top_n"


def build_sql_prompt(question: str, intent: dict) -> str:
    has_window = intent.get("has_time_window", False)
    date_col = intent.get("date_column") or "REQUEST_DATE"
    top_n = intent.get("top_n")

    head = (
        "Return Oracle SQL only inside a fenced block:\n"
        "```sql\n"
        "-- SQL starts on next line\n"
        'Table: "Contract"\n'
        f"Allowed columns: {_ALLOWED_COLS}\n"
        "Oracle syntax only (NVL, TRIM, LISTAGG WITHIN GROUP, FETCH FIRST N ROWS ONLY). SELECT/CTE only.\n"
        f"Allowed binds: {_WHITELIST_BINDS}\n"
        "Add date filter ONLY if user asks. For windows use :date_start and :date_end.\n"
        f"Default window column: {date_col}.\n"
        "No prose, comments, or explanations.\n"
        f"Question:\n{question}\n```sql\n"
    )

    tail_hint = ""
    if top_n and isinstance(top_n, int):
        tail_hint = f"-- Prefer FETCH FIRST {top_n} ROWS ONLY if needed\n"

    return head + tail_hint


# ------------- SQL Extractor -------------

_FENCE_RE = re.compile(r"```sql\s*(.*?)```", re.S | re.I)


def extract_sql_fenced(text: str) -> str:
    if not text:
        return ""
    m = _FENCE_RE.search(text)
    if m:
        sql = m.group(1).strip()
        if re.match(r"^\s*(SELECT|WITH)\b", sql, re.I):
            return sql
        return ""
    m2 = re.search(r"\b(SELECT|WITH)\b.*", text, re.S | re.I)
    return m2.group(0).strip() if m2 else ""


# ------------- NL → SQL (two-pass with repair) -------------

def nl_to_sql_with_llm(question: str, ctx: dict) -> dict:
    dbg = {"prompt": None, "raw1": None, "sql1": None, "validation1": None, "raw2": None, "sql2": None}
    sql_mdl = get_model("sql")
    if not sql_mdl:
        raise RuntimeError("SQL model not available")

    intent_out = clarify_intent(question, ctx)
    intent = intent_out["intent"]
    dbg["clarifier"] = intent_out

    prompt = build_sql_prompt(question, intent)
    dbg["prompt"] = prompt[:1000]

    raw1 = sql_mdl.generate(prompt, max_new_tokens=int(os.getenv("SQL_MAX_NEW_TOKENS", "384")), stop=["```"])
    dbg["raw1"] = raw1[:1000] if raw1 else ""
    sql1 = extract_sql_fenced(raw1)
    dbg["sql1"] = sql1

    ok1 = bool(sql1) and re.match(r"^\s*(SELECT|WITH)\b", sql1, re.I)
    dbg["validation1"] = {"ok": bool(ok1), "errors": [] if ok1 else ["empty_sql"], "binds": []}

    if ok1:
        return {"sql": sql1, "intent": intent, "debug": dbg}

    repair = (
        "Previous SQL had validation errors: ['empty_sql']\n\n"
        "Repair the SQL. Return Oracle SQL only inside a ```sql fenced block. No prose. No comments. "
        'Table: "Contract".\n'
        f"Allowed columns: {_ALLOWED_COLS}\n"
        f"Allowed binds: {_WHITELIST_BINDS}\n"
        "Use :date_start and :date_end only if a time window is requested.\n\n"
        f"Question:\n{question}\n\n"
        "```sql\n"
    )
    raw2 = sql_mdl.generate(repair, max_new_tokens=int(os.getenv("SQL_MAX_NEW_TOKENS", "256")), stop=["```"])
    dbg["raw2"] = raw2[:1000] if raw2 else ""
    sql2 = extract_sql_fenced(raw2)
    dbg["sql2"] = sql2

    if sql2 and re.match(r"^\s*(SELECT|WITH)\b", sql2, re.I):
        return {"sql": sql2, "intent": intent, "debug": dbg}

    return {"sql": "", "intent": intent, "debug": dbg}
