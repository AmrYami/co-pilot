import re
import json
import datetime as dt
from typing import Dict, Any, Optional, Tuple

from core.model_loader import get_model

_SQL_FENCE = re.compile(r"```sql\s*(.*?)```", re.IGNORECASE | re.DOTALL)
_SQL_START = re.compile(r"\b(SELECT|WITH)\b", re.IGNORECASE)


def _extract_json_between(
    text: str,
    start_tag: str = "<<JSON>>",
    end_tag: str = "<</JSON>>",
) -> Optional[Dict[str, Any]]:
    try:
        i = text.find(start_tag)
        j = text.find(end_tag)
        if i != -1 and j != -1 and j > i:
            payload = text[i + len(start_tag) : j].strip()
            if payload:
                return json.loads(payload)
    except Exception:
        pass
    return None


def _clarifier_heuristics(q: str) -> Dict[str, Any]:
    qlow = (q or "").lower()
    has_window = any(
        w in qlow for w in ["last month", "next 30 days", "last 30 days", "in 2024", "between"]
    )
    top_n: Optional[int] = None
    m = re.search(r"\btop\s+(\d+)\b", qlow)
    if m:
        try:
            top_n = int(m.group(1))
        except Exception:
            top_n = None
    date_col: Optional[str] = None
    if "end date" in qlow or "end_date" in qlow:
        date_col = "END_DATE"
    elif "start date" in qlow or "start_date" in qlow:
        date_col = "START_DATE"
    elif "request date" in qlow or "request_date" in qlow:
        date_col = "REQUEST_DATE"
    return {
        "has_time_window": has_window,
        "date_column": date_col,
        "top_n": top_n,
        "explicit_dates": None,
    }


def clarify_intent(question: str, context: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
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
        "<<JSON>>\n"
        '{"has_time_window": null, "date_column": null, "top_n": null, "explicit_dates": null}'
        "\n<</JSON>>"
    )
    raw = ""
    if mdl is not None:
        try:
            raw = mdl.generate(
                prompt,
                max_new_tokens=128,
                temperature=0.0,
                stop=["<</JSON>>"],
            )
        except Exception:
            raw = ""
    obj = _extract_json_between(raw or "")
    if not obj or not isinstance(obj, dict) or all(v is None for v in obj.values()):
        obj = _clarifier_heuristics(question)
    obj.setdefault("has_time_window", False)
    obj.setdefault("date_column", None)
    obj.setdefault("top_n", None)
    obj.setdefault("explicit_dates", None)
    dbg = {
        "ok": True,
        "used": True,
        "raw": raw[:1500] if raw else "",
        "intent": obj,
    }
    return obj, dbg


def _build_sql_prompt(question: str, date_hint: Dict[str, Any]) -> str:
    lines = []
    lines.append("Return Oracle SQL only inside ```sql fenced block.")
    lines.append('Table: "Contract"')
    lines.append(
        "Allowed columns: CONTRACT_ID, CONTRACT_OWNER, CONTRACT_STAKEHOLDER_1, CONTRACT_STAKEHOLDER_2, "
        "CONTRACT_STAKEHOLDER_3, CONTRACT_STAKEHOLDER_4, CONTRACT_STAKEHOLDER_5, CONTRACT_STAKEHOLDER_6, "
        "CONTRACT_STAKEHOLDER_7, CONTRACT_STAKEHOLDER_8, DEPARTMENT_1, DEPARTMENT_2, DEPARTMENT_3, DEPARTMENT_4, "
        "DEPARTMENT_5, DEPARTMENT_6, DEPARTMENT_7, DEPARTMENT_8, OWNER_DEPARTMENT, CONTRACT_VALUE_NET_OF_VAT, VAT, "
        "CONTRACT_PURPOSE, CONTRACT_SUBJECT, START_DATE, END_DATE, REQUEST_DATE, REQUEST_TYPE, CONTRACT_STATUS, "
        "ENTITY_NO, REQUESTER"
    )
    lines.append(
        "Oracle only: NVL, TRIM, UPPER, LISTAGG ... WITHIN GROUP, FETCH FIRST N ROWS ONLY. SELECT/CTE only."
    )
    lines.append(
        "Allowed binds: contract_id_pattern, date_end, date_start, dept, entity_no, owner_name, request_type, top_n"
    )
    lines.append(
        "Add date filter ONLY if user asks; when used, bind :date_start, :date_end. If no column named, default REQUEST_DATE."
    )
    lines.append("")
    lines.append("Question:")
    lines.append(question)
    lines.append("")
    lines.append("```sql")
    return "\n".join(lines)


def _extract_sql_only(text: str) -> str:
    if not text:
        return ""
    match = _SQL_FENCE.search(text)
    if match:
        sql = match.group(1).strip()
    else:
        match2 = _SQL_START.search(text)
        if not match2:
            return ""
        sql = text[match2.start() :].strip()
    cleaned_lines = []
    for line in sql.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("--"):
            continue
        lowered = stripped.lower()
        if any(
            tag in lowered
            for tag in [
                "return oracle sql",
                "allowed columns",
                "allowed binds",
                "fenced block",
                "question:",
            ]
        ):
            continue
        cleaned_lines.append(stripped)
    return "\n".join(cleaned_lines).strip().strip("`")


def nl_to_sql_with_llm(question: str, ctx: Dict[str, Any]) -> Dict[str, Any]:
    debug: Dict[str, Any] = {}
    intent, clar_dbg = clarify_intent(question, ctx)
    debug["clarifier"] = clar_dbg

    prompt = _build_sql_prompt(question, intent)
    debug["prompt"] = "sql_prompt_compact"

    mdl = get_model("sql")
    if mdl is None:
        return {"ok": False, "sql": "", "debug": debug, "error": "model_unavailable"}

    raw1 = mdl.generate(
        prompt,
        max_new_tokens=192,
        temperature=0.05,
        top_p=0.9,
        stop=["```"]
    )
    debug["raw1"] = raw1[:1500] if raw1 else ""
    sql1 = _extract_sql_only(raw1)
    debug["sql1"] = sql1

    from .validator import validate_sql

    validation1 = validate_sql(sql1)
    debug["validation1"] = validation1

    if not validation1.get("ok") or not sql1:
        repair_prompt = (
            "Previous SQL had validation errors:\n"
            f"{json.dumps(validation1.get('errors', []))}\n\n"
            "Repair the SQL. Return Oracle SQL only inside a fenced block. No prose. No comments.\n"
            'Table: "Contract"\n'
            "Use only allowed columns and binds. Use :date_start/:date_end only when a window is asked.\n\n"
            f"Question:\n{question}\n\n"
            "```sql\n"
        )
        debug["sql_repair_prompt"] = "sql_prompt_compact"
        raw2 = mdl.generate(
            repair_prompt,
            max_new_tokens=160,
            temperature=0.05,
            top_p=0.9,
            stop=["```"]
        )
        debug["raw2"] = raw2[:1500] if raw2 else ""
        sql2 = _extract_sql_only(raw2)
        debug["sql2"] = sql2
        validation2 = validate_sql(sql2)
        debug["validation2"] = validation2
        if validation2.get("ok") and sql2:
            return {"ok": True, "sql": sql2, "debug": debug}
        return {"ok": False, "sql": sql2, "debug": debug, "error": "validation_failed"}

    return {"ok": True, "sql": sql1, "debug": debug}


__all__ = ["clarify_intent", "nl_to_sql_with_llm"]
