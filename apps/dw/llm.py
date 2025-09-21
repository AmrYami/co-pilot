"""LLM utilities for the DW application."""

from __future__ import annotations

import json
import logging
import os
import re

from core.model_loader import get_model

log = logging.getLogger("main")


# ------------- Clarifier -------------

_JSON_RE = re.compile(r"<<JSON>>\s*(\{.*?\})\s*<</JSON>>", re.S)


def _heuristic_intent(question: str) -> dict:
    q = (question or "").lower()
    has_window = any(
        w in q for w in ["last month", "next 30 days", "in 2024", "between", "since "]
    )
    date_col = "END_DATE" if any(w in q for w in ["expire", "expiry", "end date"]) else "REQUEST_DATE"
    top_n = None
    m = re.search(r"\btop\s+(\d+)\b", q)
    if m:
        try:
            top_n = int(m.group(1))
        except Exception:
            top_n = None
    return {
        "has_time_window": bool(has_window),
        "date_column": (
            date_col
            if has_window
            or "end date" in q
            or "request date" in q
            or "start date" in q
            else None
        ),
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


# ------------- NL → SQL -------------


def nl_to_sql_with_llm(question: str, ctx: dict) -> dict:
    """
    Two-pass SQL generation + validation.
    - pass1: compact prompt
    - if empty/invalid: pass2 repair prompt
    Returns { raw1, sql1, raw2, sql2, final_sql } plus debug bits.
    """
    clarifier = clarify_intent(question, ctx)
    intent = clarifier.get("intent") or {}

    sql_mdl = get_model("sql")  # SQLCoderExLlama from the wrapper above
    max_new_tokens = int(os.getenv("SQL_MAX_NEW_TOKENS", os.getenv("GENERATION_MAX_NEW_TOKENS", "192")))

    # --------- Prompt 1 (compact) ----------
    prompt1 = (
        "Return Oracle SQL only inside ```sql fenced block. No prose.\n\n"
        'Table: "Contract"\n'
        "Allowed columns only: CONTRACT_ID, CONTRACT_OWNER, CONTRACT_STAKEHOLDER_1, CONTRACT_STAKEHOLDER_2, "
        "CONTRACT_STAKEHOLDER_3, CONTRACT_STAKEHOLDER_4, CONTRACT_STAKEHOLDER_5, CONTRACT_STAKEHOLDER_6, "
        "CONTRACT_STAKEHOLDER_7, CONTRACT_STAKEHOLDER_8, DEPARTMENT_1, DEPARTMENT_2, DEPARTMENT_3, DEPARTMENT_4, "
        "DEPARTMENT_5, DEPARTMENT_6, DEPARTMENT_7, DEPARTMENT_8, OWNER_DEPARTMENT, CONTRACT_VALUE_NET_OF_VAT, VAT, "
        "CONTRACT_PURPOSE, CONTRACT_SUBJECT, START_DATE, END_DATE, REQUEST_DATE, REQUEST_TYPE, CONTRACT_STATUS, "
        "ENTITY_NO, REQUESTER\n"
        "Oracle syntax: NVL, TRIM, LISTAGG ... WITHIN GROUP, FETCH FIRST N ROWS ONLY. SELECT/CTE only.\n"
        "Allowed binds: contract_id_pattern, date_end, date_start, dept, entity_no, owner_name, request_type, top_n\n"
        "Do not add any date filter unless the user explicitly requests a time window.\n\n"
        f"Question:\n{question}\n\n```sql\n"
    )

    out1 = sql_mdl.generate_sql(prompt1, max_new_tokens=max_new_tokens)
    raw1, sql1 = out1["raw"], out1["sql"]
    if os.getenv("DW_DEBUG", "0") == "1":
        log.info("[dw] llm_raw_pass1: size=%s", len(raw1))
        log.info("[dw] llm_sql_pass1: preview=%s", (sql1[:120] if sql1 else ""))

    if sql1:
        return {
            "raw1": raw1,
            "sql1": sql1,
            "final_sql": sql1,
            "pass": 1,
            "intent": intent,
            "clarifier": clarifier,
            "ok": True,
            "errors": [],
        }

    # --------- Prompt 2 (repair) ----------
    prompt2 = (
        "Fix the SQL. Return only Oracle SQL in ```sql fenced block. No prose.\n\n"
        'Table: "Contract"\n'
        "Allowed columns only: CONTRACT_ID, CONTRACT_OWNER, CONTRACT_STAKEHOLDER_1, CONTRACT_STAKEHOLDER_2, "
        "CONTRACT_STAKEHOLDER_3, CONTRACT_STAKEHOLDER_4, CONTRACT_STAKEHOLDER_5, CONTRACT_STAKEHOLDER_6, "
        "CONTRACT_STAKEHOLDER_7, CONTRACT_STAKEHOLDER_8, DEPARTMENT_1, DEPARTMENT_2, DEPARTMENT_3, DEPARTMENT_4, "
        "DEPARTMENT_5, DEPARTMENT_6, DEPARTMENT_7, DEPARTMENT_8, OWNER_DEPARTMENT, CONTRACT_VALUE_NET_OF_VAT, VAT, "
        "CONTRACT_PURPOSE, CONTRACT_SUBJECT, START_DATE, END_DATE, REQUEST_DATE, REQUEST_TYPE, CONTRACT_STATUS, "
        "ENTITY_NO, REQUESTER\n"
        "Oracle syntax only. SELECT/CTE only. Allowed binds as above.\n\n"
        f"Question:\n{question}\n\n"
        "Previous attempt:\n```sql\n" + (sql1 or "") + "\n```\n"
        "Answer:\n```sql\n"
    )

    out2 = sql_mdl.generate_sql(prompt2, max_new_tokens=max_new_tokens)
    raw2, sql2 = out2["raw"], out2["sql"]
    if os.getenv("DW_DEBUG", "0") == "1":
        log.info("[dw] llm_raw_pass2: size=%s", len(raw2))
        log.info("[dw] llm_sql_pass2: preview=%s", (sql2[:120] if sql2 else ""))

    final_sql = sql2 or ""
    return {
        "raw1": raw1,
        "sql1": sql1,
        "raw2": raw2,
        "sql2": sql2,
        "final_sql": final_sql,
        "pass": 2 if final_sql else 0,
        "intent": intent,
        "clarifier": clarifier,
        "ok": bool(final_sql),
        "errors": [] if final_sql else ["empty_sql"],
    }
