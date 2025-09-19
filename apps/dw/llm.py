import re

from core.model_loader import get_model

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


def implies_time_window(q: str) -> bool:
    if not q:
        return False
    return bool(
        re.search(
            r"\b(next|last|previous|past|\d+\s*(day|days|week|weeks|month|months|year|years)|today|yesterday|between|from\s+\d|before|after|up to)\b",
            q,
            re.IGNORECASE,
        )
    )


def build_prompt(question: str, allow_window: bool) -> str:
    base = [
        "Return exactly one Oracle SQL query.",
        "No comments. No prose. No explanations. SELECT/CTE only.",
        'Use table "Contract" only.',
        "You may use these columns only: " + ", ".join(ALLOWED_COLUMNS) + ".",
        "Use Oracle syntax: NVL(), LISTAGG(... WITHIN GROUP (...)), TRIM(), UPPER(), FETCH FIRST N ROWS ONLY.",
    ]
    if allow_window:
        base += [
            "If (and only if) the user explicitly requests a time window, add a WHERE filter on the mentioned date column",
            "and use named binds :date_start and :date_end (e.g., column >= :date_start AND column < :date_end).",
        ]
    else:
        base += ["Do not add any date filter unless the question asks for one."]
    base += ["SQL:"]
    return "\n".join(base) + "\n" + question.strip() + "\nSQL:"


def extract_sql_only(text: str) -> str | None:
    if not text:
        return None
    cleaned = re.sub(r"```(?:sql)?", "", text, flags=re.IGNORECASE).strip()
    match = re.search(r"\b(SELECT|WITH)\b", cleaned, flags=re.IGNORECASE)
    if not match:
        return None
    sql = cleaned[match.start() :].strip()
    sql = sql.split("```", 1)[0].strip()
    return sql.rstrip(";").strip()


def generate_sql_oracle(question: str) -> dict:
    allow_window = implies_time_window(question)
    prompt = build_prompt(question, allow_window)
    model = get_model("sql")
    if model is None:
        return {"sql": None, "used_window": allow_window, "reason": "no_model"}
    try:
        output = model.generate(prompt)
    except Exception:
        return {"sql": None, "used_window": allow_window, "reason": "error"}
    sql = extract_sql_only(output)
    return {
        "sql": sql,
        "used_window": allow_window,
        "reason": "ok" if sql else "empty_sql",
    }
