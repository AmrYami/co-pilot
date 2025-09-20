import json
import logging
from core.model_loader import get_model
from .validator import extract_sql_from_fenced, validate_sql

logger = logging.getLogger("dw")

ALLOWED_BINDS = {"date_start","date_end","top_n","owner_name","dept","entity_no","contract_id_pattern","request_type"}

def build_sql_prompt(question: str, context: dict) -> str:
    # Small, strict, and asks for fenced block only
    use_window = context.get("has_time_window", False)
    default_date_col = context.get("date_column") or "REQUEST_DATE"

    head = (
        "Return Oracle SQL only inside ```sql fenced block.\n"
        "Table: \"Contract\"\n"
        "Allowed columns: CONTRACT_ID, CONTRACT_OWNER, CONTRACT_STAKEHOLDER_1, CONTRACT_STAKEHOLDER_2, "
        "CONTRACT_STAKEHOLDER_3, CONTRACT_STAKEHOLDER_4, CONTRACT_STAKEHOLDER_5, CONTRACT_STAKEHOLDER_6, "
        "CONTRACT_STAKEHOLDER_7, CONTRACT_STAKEHOLDER_8, DEPARTMENT_1, DEPARTMENT_2, DEPARTMENT_3, DEPARTMENT_4, "
        "DEPARTMENT_5, DEPARTMENT_6, DEPARTMENT_7, DEPARTMENT_8, OWNER_DEPARTMENT, CONTRACT_VALUE_NET_OF_VAT, VAT, "
        "CONTRACT_PURPOSE, CONTRACT_SUBJECT, START_DATE, END_DATE, REQUEST_DATE, REQUEST_TYPE, CONTRACT_STATUS, "
        "ENTITY_NO, REQUESTER\n"
        "Oracle syntax only (NVL, TRIM, LISTAGG WITHIN GROUP, FETCH FIRST N ROWS ONLY). SELECT/CTE only.\n"
        "Allowed binds: contract_id_pattern, date_end, date_start, dept, entity_no, owner_name, request_type, top_n\n"
        "Add date filter ONLY if the user explicitly asks for a window.\n"
        "When a window IS requested, filter using :date_start and :date_end on the correct date column.\n"
        f"Default window column: {default_date_col}.\n"
        "No prose, no comments.\n\n"
        "Question:\n"
        f"{question}\n\n"
        "```sql"
    )
    return head

def nl_to_sql_with_llm(question: str, context: dict) -> dict:
    sql_mdl = get_model("sql")  # your SQLCoder/ExLlama wrapper

    prompt = build_sql_prompt(question, context)
    logger.info("[dw] sql_prompt")

    # PASS 1
    raw1 = sql_mdl.generate(prompt, max_new_tokens=256, stop=["```"])
    logger.info("[dw] llm_raw_pass1: size=%s", len(raw1 or ""))
    sql1 = extract_sql_from_fenced(raw1)
    val1 = validate_sql(sql1, ALLOWED_BINDS)
    logger.info("[dw] llm_sql_pass1")

    if val1["ok"]:
        return {
            "prompt": prompt,
            "pass": 1,
            "raw1": raw1,
            "sql1": sql1,
            "val1": val1,
            "raw2": None,
            "sql2": None,
            "val2": None,
            "final_sql": sql1
        }

    # PASS 2 (Repair)
    repair_prompt = (
        "Previous SQL had validation errors:\n"
        f"{json.dumps(val1['errors'])}\n\n"
        "Repair the SQL. Return Oracle SQL only inside a fenced block. No prose. No comments.\n"
        "Rules:\n"
        ' - Table: "Contract"\n'
        " - Allowed columns only: (same as before)\n"
        " - Allowed binds only: (same as before)\n"
        " - Use :date_start and :date_end when a window is requested.\n\n"
        "Question:\n"
        f"{question}\n\n"
        "```sql"
    )
    logger.info("[dw] sql_prompt_repair")
    raw2 = sql_mdl.generate(repair_prompt, max_new_tokens=192, stop=["```"])
    logger.info("[dw] llm_raw_pass2: size=%s", len(raw2 or ""))
    sql2 = extract_sql_from_fenced(raw2)
    val2 = validate_sql(sql2, ALLOWED_BINDS)
    logger.info("[dw] llm_sql_pass2")

    final_sql = sql2 if val2["ok"] else sql1  # prefer repaired if valid
    return {
        "prompt": prompt,
        "pass": 2 if val2["ok"] else 1,
        "raw1": raw1, "sql1": sql1, "val1": val1,
        "raw2": raw2, "sql2": sql2, "val2": val2,
        "final_sql": final_sql
    }
