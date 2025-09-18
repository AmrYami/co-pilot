import os
import re
from typing import Any, Dict, Optional, Tuple

# Import the module (not a missing symbol) to avoid ImportError
import core.model_loader as model_loader


# --------- env helpers (we keep all model knobs in .env as you requested) ---------
def _get_env_str(key: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(key)
    return v if (v is not None and v != "") else default


def _get_env_int(key: str, default: int) -> int:
    try:
        return int(os.getenv(key, str(default)))
    except Exception:
        return default


def _get_env_float(key: str, default: float) -> float:
    try:
        return float(os.getenv(key, str(default)))
    except Exception:
        return default


# --------- minimal loader wrapper (keeps your existing model loader intact) ---------
_LLMCACHE: Dict[str, Any] = {"handle": None, "meta": None}


def get_llm() -> Tuple[Any, Dict[str, Any]]:
    """Returns (generator_handle, meta_dict).
    - Does NOT read mem_settings. Uses only .env, per your direction.
    - Delegates to whichever base loader your repo already exposes.
    """

    if _LLMCACHE["handle"] is not None:
        return _LLMCACHE["handle"], _LLMCACHE["meta"]

    backend = _get_env_str("MODEL_BACKEND", "exllama")
    model_path = _get_env_str("MODEL_PATH", "")
    max_len = _get_env_int("MODEL_MAX_SEQ_LEN", 4096)
    temperature = _get_env_float("GENERATION_TEMPERATURE", 0.2)
    top_p = _get_env_float("GENERATION_TOP_P", 0.9)
    stop_str = _get_env_str("STOP", "</s>,<|im_end|>")

    # Find a usable loader function in core.model_loader without changing it
    candidates = [
        "load_model",  # many repos export this
        "get_base_generator",  # sometimes used
        "load_main",  # sometimes used
        "load_base",  # sometimes used
    ]
    loader_fn = None
    for name in candidates:
        loader_fn = getattr(model_loader, name, None)
        if callable(loader_fn):
            break
    if loader_fn is None:
        # As a last resort, try a private helper that exists in your repo
        loader_fn = getattr(model_loader, "_load_exllama", None)
        if not callable(loader_fn):
            raise RuntimeError(
                "No LLM loader available in core.model_loader. "
                "Expected one of: load_model, get_base_generator, load_main, load_base, _load_exllama."
            )

    handle = None
    try:
        handle = loader_fn()
    except TypeError:
        handle = loader_fn(None) if model_path else loader_fn()

    if handle is None:
        raise RuntimeError("LLM handle is None after loading.")

    meta_from_handle = getattr(handle, "meta", {}) or {}
    backend_from_handle = (
        meta_from_handle.get("backend")
        or getattr(handle, "backend", None)
        or backend
    )

    meta: Dict[str, Any] = {
        "backend": backend_from_handle,
        "path": meta_from_handle.get("model_path") or model_path,
        "max_seq_len": meta_from_handle.get("model_max_seq_len") or max_len,
        "temperature": temperature,
        "top_p": top_p,
        "stop": stop_str,
    }
    _LLMCACHE["handle"] = handle
    _LLMCACHE["meta"] = meta
    return handle, meta


# --------- DW-specific prompt (Oracle, one table, your important columns) ---------
DW_SYSTEM_PROMPT = """\
You translate natural language into **valid Oracle SQL** against a single table named "Contract".

**Strict rules**
- Return ONLY SQL. Do not add comments, explanations, or Markdown.
- Use only SELECT and WITH (CTE). No DDL/DML/PLSQL.
- String match is case-insensitive when using UPPER(...) comparisons.
- Use Oracle functions: NVL, TRIM, LISTAGG, EXTRACT(YEAR FROM ...), ADD_MONTHS, CURRENT_DATE, etc.
- Dates: The table contains DATE and TIMESTAMP columns (e.g., START_DATE, END_DATE, REQUEST_DATE).
- If a time window like "last month" is requested, expect bound parameters :date_start and :date_end (half-open range).
- For “contract value (gross)”, use NVL(CONTRACT_VALUE_NET_OF_VAT,0) + NVL(VAT,0).

**Table**: "Contract"
**Important columns**
- CONTRACT_ID (NVARCHAR2)
- CONTRACT_OWNER (NVARCHAR2)
- CONTRACT_STAKEHOLDER_1..8 (NVARCHAR2) paired with DEPARTMENT_1..8 (NVARCHAR2)
- OWNER_DEPARTMENT (NVARCHAR2)
- CONTRACT_VALUE_NET_OF_VAT (NUMBER), VAT (NUMBER)
- CONTRACT_PURPOSE, CONTRACT_SUBJECT (NVARCHAR2)
- START_DATE (DATE), END_DATE (DATE), REQUEST_DATE (DATE)
- DURATION, ENTITY, BUILDING_AND_FLOOR_DESCRIPTION, LEGAL_NAME_OF_THE_COMPANY, REPRESENTATIVE_* (NVARCHAR2)
- VALUE_DESCRIPTION, YEAR, CONTRACTOR_ID, REQUEST_ID, CONTRACT_STATUS, REQUEST_TYPE,
  DEPARTMENT_OUL, CONTRACT_ID_COUNTER, REQUESTER, EXPIERY_30/60/90 (DATE), ENTITY_NO

**Stakeholder/Department pairing**
- (CONTRACT_STAKEHOLDER_1, DEPARTMENT_1), (CONTRACT_STAKEHOLDER_2, DEPARTMENT_2), … up to 8.
- To aggregate by stakeholder across all slots, UNION ALL the 8 pairs into one derived set and group.

**Examples of safe patterns**
- Half-open time window:
  WHERE REF_DATE >= :date_start AND REF_DATE < :date_end

- Top-N with Oracle 12+:
  ORDER BY total_value DESC
  FETCH FIRST 10 ROWS ONLY
"""

# Guidance for transforming natural language intent into an Oracle SQL skeleton
DW_FEW_SHOT = """\
Q: top 10 stakeholders by contract value last month
SQL:
WITH stakeholders AS (
  SELECT CONTRACT_ID,
         NVL(CONTRACT_VALUE_NET_OF_VAT,0) + NVL(VAT,0) AS CONTRACT_VALUE_GROSS,
         CONTRACT_STAKEHOLDER_1 AS STAKEHOLDER,
         DEPARTMENT_1 AS DEPARTMENT,
         REQUEST_DATE AS REF_DATE
    FROM "Contract"
  UNION ALL SELECT CONTRACT_ID, NVL(CONTRACT_VALUE_NET_OF_VAT,0)+NVL(VAT,0), CONTRACT_STAKEHOLDER_2, DEPARTMENT_2, REQUEST_DATE FROM "Contract"
  UNION ALL SELECT CONTRACT_ID, NVL(CONTRACT_VALUE_NET_OF_VAT,0)+NVL(VAT,0), CONTRACT_STAKEHOLDER_3, DEPARTMENT_3, REQUEST_DATE FROM "Contract"
  UNION ALL SELECT CONTRACT_ID, NVL(CONTRACT_VALUE_NET_OF_VAT,0)+NVL(VAT,0), CONTRACT_STAKEHOLDER_4, DEPARTMENT_4, REQUEST_DATE FROM "Contract"
  UNION ALL SELECT CONTRACT_ID, NVL(CONTRACT_VALUE_NET_OF_VAT,0)+NVL(VAT,0), CONTRACT_STAKEHOLDER_5, DEPARTMENT_5, REQUEST_DATE FROM "Contract"
  UNION ALL SELECT CONTRACT_ID, NVL(CONTRACT_VALUE_NET_OF_VAT,0)+NVL(VAT,0), CONTRACT_STAKEHOLDER_6, DEPARTMENT_6, REQUEST_DATE FROM "Contract"
  UNION ALL SELECT CONTRACT_ID, NVL(CONTRACT_VALUE_NET_OF_VAT,0)+NVL(VAT,0), CONTRACT_STAKEHOLDER_7, DEPARTMENT_7, REQUEST_DATE FROM "Contract"
  UNION ALL SELECT CONTRACT_ID, NVL(CONTRACT_VALUE_NET_OF_VAT,0)+NVL(VAT,0), CONTRACT_STAKEHOLDER_8, DEPARTMENT_8, REQUEST_DATE FROM "Contract"
)
SELECT TRIM(STAKEHOLDER) AS stakeholder,
       SUM(CONTRACT_VALUE_GROSS) AS total_gross_value,
       COUNT(DISTINCT CONTRACT_ID) AS contract_count,
       LISTAGG(DISTINCT TRIM(DEPARTMENT), ', ') WITHIN GROUP (ORDER BY TRIM(DEPARTMENT)) AS departments
  FROM stakeholders
 WHERE STAKEHOLDER IS NOT NULL
   AND TRIM(STAKEHOLDER) <> ''
   AND REF_DATE >= :date_start
   AND REF_DATE < :date_end
 GROUP BY TRIM(STAKEHOLDER)
 ORDER BY total_gross_value DESC
 FETCH FIRST 10 ROWS ONLY
"""


def _compose_prompt(question: str) -> str:
    # Simple single-turn prompt—works well with SQLCoder style models
    return f"{DW_SYSTEM_PROMPT}\n\n{DW_FEW_SHOT}\n\nQ: {question}\nSQL:\n"


# --------- NL → SQL using the loaded generator ---------
_SQL_ONLY = re.compile(r"(?is)\b(select|with)\b")


def nl_to_sql_with_llm(question: str) -> str:
    gen, _ = get_llm()

    prompt = _compose_prompt(question)

    gen_kwargs = {
        "max_new_tokens": _get_env_int("GENERATION_MAX_NEW_TOKENS", 256),
        "temperature": _get_env_float("GENERATION_TEMPERATURE", 0.2),
        "top_p": _get_env_float("GENERATION_TOP_P", 0.9),
        "stop": [
            s.strip()
            for s in (_get_env_str("STOP", "</s>,<|im_end|>") or "").split(",")
            if s.strip()
        ]
        or None,
    }
    if hasattr(gen, "generate"):
        text = gen.generate(prompt, **gen_kwargs)
    else:
        text = gen(prompt, **gen_kwargs)

    sql = str(text or "").strip()
    match = _SQL_ONLY.search(sql)
    if not match:
        return ""
    if match.start() > 0:
        sql = sql[match.start() :].strip()
    semi = sql.find(";")
    if semi != -1:
        sql = sql[:semi].strip()
    return sql
