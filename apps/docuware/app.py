"""DocuWare-specific Flask blueprint."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from flask import Blueprint, jsonify, request
from sqlalchemy import text

from core.inquiries import create_or_update_inquiry
from core.pipeline import Pipeline
from core.seed import upsert_metrics, upsert_mappings, upsert_snippet
from core.settings import Settings
from core.sql_exec import run_sql

blueprint = Blueprint("docuware", __name__)

NAMESPACE = "dw::common"
_PIPELINE: Optional[Pipeline] = None
_SETTINGS: Optional[Settings] = None


@dataclass
class DocuwarePlan:
    ok: bool
    sql: Optional[str] = None
    rationale: Optional[str] = None
    questions: Optional[List[str]] = None
    error: Optional[str] = None


def _get_settings() -> Settings:
    global _SETTINGS
    if _SETTINGS is None:
        _SETTINGS = Settings(namespace=NAMESPACE)
    return _SETTINGS


def _get_pipeline() -> Pipeline:
    global _PIPELINE
    if _PIPELINE is None:
        settings = _get_settings()
        _PIPELINE = Pipeline(settings=settings, namespace=NAMESPACE)
    return _PIPELINE


def _coerce_prefixes(raw) -> List[str]:
    if not raw:
        return []
    if isinstance(raw, (list, tuple, set)):
        return [str(p) for p in raw if p is not None]
    return [str(raw)]


def _lookup_snippet_sql(pipe: Pipeline, title: str) -> Optional[str]:
    with pipe.mem_engine.begin() as conn:
        row = conn.execute(
            text(
                """
                SELECT sql_raw
                  FROM mem_snippets
                 WHERE namespace = :ns AND title = :title
                 ORDER BY updated_at DESC
                 LIMIT 1
                """
            ),
            {"ns": NAMESPACE, "title": title},
        ).scalar()
    return (row or "").strip() if row else None


def _plan_from_question(pipe: Pipeline, question: str) -> DocuwarePlan:
    q = (question or "").lower()
    snippet_titles: List[str] = []
    if "stakeholder" in q:
        snippet_titles.append("Top stakeholders by gross value (last 12 months)")
    if "expir" in q:
        snippet_titles.append("Contracts expiring in next 90 days (buckets)")
    if "department" in q or "owner department" in q:
        snippet_titles.append("Gross value by department (this year)")

    for title in snippet_titles:
        sql = _lookup_snippet_sql(pipe, title)
        if sql:
            return DocuwarePlan(ok=True, sql=sql, rationale=f"Used seeded snippet '{title}'.")

    return DocuwarePlan(
        ok=False,
        questions=[
            "Could you clarify whether you need stakeholders, expiry buckets, or departmental totals?",
            "Specify the desired date window (e.g., last 12 months, next 90 days).",
        ],
        error="no_matching_snippet",
    )


@blueprint.route("/dw/answer", methods=["POST"])
def answer():
    data = request.get_json(force=True, silent=True) or {}
    question = (data.get("question") or "").strip()
    if not question:
        return jsonify({"ok": False, "error": "missing_question"}), 400

    prefixes = _coerce_prefixes(data.get("prefixes"))
    auth_email = data.get("auth_email")

    pipe = _get_pipeline()
    datasource = (
        data.get("datasource")
        or pipe.default_ds
        or pipe.settings.default_datasource(NAMESPACE)
        or "default"
    )
    try:
        research_enabled = pipe.settings.research_allowed(datasource, namespace=NAMESPACE)
    except Exception:
        research_enabled = bool(pipe.settings.get("RESEARCH_MODE", namespace=NAMESPACE))

    inquiry_id = create_or_update_inquiry(
        pipe.mem_engine,
        namespace=NAMESPACE,
        prefixes=prefixes,
        question=question,
        auth_email=auth_email,
        run_id=None,
        research_enabled=research_enabled,
        datasource=datasource,
        status="open",
    )

    plan = _plan_from_question(pipe, question)
    if not plan.ok or not plan.sql:
        payload = {
            "inquiry_id": inquiry_id,
            "status": "needs_clarification",
            "questions": plan.questions or [],
        }
        if plan.error:
            payload["error"] = plan.error
        return jsonify(payload)

    engine = pipe.ds.engine(datasource)
    result = run_sql(engine, plan.sql)
    response = {
        "inquiry_id": inquiry_id,
        "status": "answered" if result.ok else "failed",
        "sql": plan.sql,
        "result": result.dict(),
        "rationale": plan.rationale,
    }
    if result.ok and not result.rows:
        response["note"] = (
            "No rows returned. Try a wider date window (e.g., last 12 months) or check filters."
        )
    if not result.ok and result.error:
        response["error"] = result.error
    return jsonify(response), (200 if result.ok else 500)


@blueprint.route("/dw/seed", methods=["POST"])
def seed():
    pipe = _get_pipeline()

    metrics = [
        {
            "metric_key": "gross_contract_value",
            "metric_name": "Gross Contract Value",
            "description": "NVL(CONTRACT_VALUE_NET_OF_VAT,0)+NVL(VAT,0)",
            "calculation_sql": "NVL(CONTRACT_VALUE_NET_OF_VAT,0) + NVL(VAT,0)",
            "required_tables": ["Contract"],
            "required_columns": ["CONTRACT_VALUE_NET_OF_VAT", "VAT"],
            "category": "contracts",
        },
        {
            "metric_key": "net_contract_value",
            "metric_name": "Net Contract Value",
            "calculation_sql": "NVL(CONTRACT_VALUE_NET_OF_VAT,0)",
            "required_tables": ["Contract"],
            "required_columns": ["CONTRACT_VALUE_NET_OF_VAT"],
            "category": "contracts",
        },
        {
            "metric_key": "vat_amount",
            "metric_name": "VAT Amount",
            "calculation_sql": "NVL(VAT,0)",
            "required_tables": ["Contract"],
            "required_columns": ["VAT"],
            "category": "contracts",
        },
        {
            "metric_key": "contract_count",
            "metric_name": "Contract Count",
            "calculation_sql": "COUNT(1)",
            "required_tables": ["Contract"],
            "required_columns": ["DWDOCID"],
            "category": "contracts",
        },
    ]
    metrics_result = upsert_metrics(pipe.mem_engine, namespace=NAMESPACE, metrics=metrics)

    mappings = [
        {
            "alias": "stakeholder",
            "canonical": "CONTRACT_STAKEHOLDER_*",
            "mapping_type": "column",
            "scope": "Contract",
        },
        {
            "alias": "department",
            "canonical": "DEPARTMENT_*",
            "mapping_type": "column",
            "scope": "Contract",
        },
        {
            "alias": "owner_department",
            "canonical": "OWNER_DEPARTMENT",
            "mapping_type": "column",
            "scope": "Contract",
        },
        {
            "alias": "value_gross",
            "canonical": "NVL(CONTRACT_VALUE_NET_OF_VAT,0)+NVL(VAT,0)",
            "mapping_type": "metric",
            "scope": "Contract",
        },
        {
            "alias": "value_net",
            "canonical": "NVL(CONTRACT_VALUE_NET_OF_VAT,0)",
            "mapping_type": "metric",
            "scope": "Contract",
        },
    ]
    mappings_result = upsert_mappings(
        pipe.mem_engine, namespace=NAMESPACE, mappings=mappings
    )

    snippet_sql_1 = """
SELECT
  p.stakeholder,
  SUM(NVL(c.CONTRACT_VALUE_NET_OF_VAT,0) + NVL(c.VAT,0)) AS gross_contract_value
FROM Contract c
CROSS APPLY (
  SELECT '1' AS slot, c.CONTRACT_STAKEHOLDER_1 AS stakeholder, c.DEPARTMENT_1 AS department FROM dual
  UNION ALL SELECT '2', c.CONTRACT_STAKEHOLDER_2, c.DEPARTMENT_2 FROM dual
  UNION ALL SELECT '3', c.CONTRACT_STAKEHOLDER_3, c.DEPARTMENT_3 FROM dual
  UNION ALL SELECT '4', c.CONTRACT_STAKEHOLDER_4, c.DEPARTMENT_4 FROM dual
  UNION ALL SELECT '5', c.CONTRACT_STAKEHOLDER_5, c.DEPARTMENT_5 FROM dual
  UNION ALL SELECT '6', c.CONTRACT_STAKEHOLDER_6, c.DEPARTMENT_6 FROM dual
  UNION ALL SELECT '7', c.CONTRACT_STAKEHOLDER_7, c.DEPARTMENT_7 FROM dual
  UNION ALL SELECT '8', c.CONTRACT_STAKEHOLDER_8, c.DEPARTMENT_8 FROM dual
) p
WHERE p.stakeholder IS NOT NULL
  AND TRUNC(c.START_DATE, 'MM') >= ADD_MONTHS(TRUNC(SYSDATE, 'MM'), -12)
GROUP BY p.stakeholder
ORDER BY gross_contract_value DESC
FETCH FIRST 10 ROWS ONLY
"""
    upsert_snippet(
        pipe.mem_engine,
        namespace=NAMESPACE,
        title="Top stakeholders by gross value (last 12 months)",
        sql_raw=snippet_sql_1,
        input_tables=["Contract"],
        tags=["dw", "contracts", "stakeholder", "top10"],
    )

    snippet_sql_2 = """
SELECT
  CASE
    WHEN c.END_DATE >= TRUNC(SYSDATE) AND c.END_DATE < TRUNC(SYSDATE) + 30 THEN 'next_30_days'
    WHEN c.END_DATE >= TRUNC(SYSDATE) AND c.END_DATE < TRUNC(SYSDATE) + 60 THEN 'next_60_days'
    WHEN c.END_DATE >= TRUNC(SYSDATE) AND c.END_DATE < TRUNC(SYSDATE) + 90 THEN 'next_90_days'
    ELSE 'other'
  END AS bucket,
  COUNT(1) AS contract_count,
  SUM(NVL(c.CONTRACT_VALUE_NET_OF_VAT,0) + NVL(c.VAT,0)) AS gross_contract_value
FROM Contract c
WHERE c.END_DATE IS NOT NULL
  AND c.END_DATE < TRUNC(SYSDATE) + 90
GROUP BY
  CASE
    WHEN c.END_DATE >= TRUNC(SYSDATE) AND c.END_DATE < TRUNC(SYSDATE) + 30 THEN 'next_30_days'
    WHEN c.END_DATE >= TRUNC(SYSDATE) AND c.END_DATE < TRUNC(SYSDATE) + 60 THEN 'next_60_days'
    WHEN c.END_DATE >= TRUNC(SYSDATE) AND c.END_DATE < TRUNC(SYSDATE) + 90 THEN 'next_90_days'
    ELSE 'other'
  END
ORDER BY 1
"""
    upsert_snippet(
        pipe.mem_engine,
        namespace=NAMESPACE,
        title="Contracts expiring in next 90 days (buckets)",
        sql_raw=snippet_sql_2,
        input_tables=["Contract"],
        tags=["dw", "contracts", "expiry", "buckets"],
    )

    snippet_sql_3 = """
SELECT
  p.department,
  SUM(NVL(c.CONTRACT_VALUE_NET_OF_VAT,0) + NVL(c.VAT,0)) AS gross_contract_value
FROM Contract c
CROSS APPLY (
  SELECT c.DEPARTMENT_1 AS department FROM dual
  UNION ALL SELECT c.DEPARTMENT_2 FROM dual
  UNION ALL SELECT c.DEPARTMENT_3 FROM dual
  UNION ALL SELECT c.DEPARTMENT_4 FROM dual
  UNION ALL SELECT c.DEPARTMENT_5 FROM dual
  UNION ALL SELECT c.DEPARTMENT_6 FROM dual
  UNION ALL SELECT c.DEPARTMENT_7 FROM dual
  UNION ALL SELECT c.DEPARTMENT_8 FROM dual
) p
WHERE p.department IS NOT NULL
  AND TRUNC(c.START_DATE, 'YYYY') = TRUNC(SYSDATE, 'YYYY')
GROUP BY p.department
ORDER BY gross_contract_value DESC
"""
    upsert_snippet(
        pipe.mem_engine,
        namespace=NAMESPACE,
        title="Gross value by department (this year)",
        sql_raw=snippet_sql_3,
        input_tables=["Contract"],
        tags=["dw", "contracts", "department", "ytd"],
    )

    return jsonify(
        {
            "ok": True,
            "metrics": metrics_result.count,
            "mappings": mappings_result.count,
            "snippets": 3,
        }
    )
