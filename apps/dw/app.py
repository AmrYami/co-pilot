from flask import Blueprint, request, jsonify
from sqlalchemy import text
import re

from core.pipeline import Pipeline


def create_dw_blueprint(pipeline: Pipeline) -> Blueprint:
    bp = Blueprint("dw", __name__)
    namespace = pipeline.namespace

    def _dw_engine():
        return pipeline.ds.engine(None)

    @bp.post("/seed")
    def seed():
        mem = pipeline.mem

        metric_net = text(
            """
            INSERT INTO mem_metrics(namespace, metric_key, metric_name, description,
                                    calculation_sql, required_tables, required_columns,
                                    category, owner, is_active, verified_at, created_at, updated_at)
            VALUES
              (:ns,'contract_value_net','Contract Value (Net of VAT)',
               'Base contract amount excluding VAT',
               'NVL(CONTRACT_VALUE_NET_OF_VAT,0)',
               '["Contract"]','["CONTRACT_VALUE_NET_OF_VAT"]',
               'contracts','system', true, NOW(), NOW(), NOW())
            ON CONFLICT (namespace, metric_key, version) DO NOTHING;
            """
        )
        metric_vat = text(
            """
            INSERT INTO mem_metrics(namespace, metric_key, metric_name, description,
                                    calculation_sql, required_tables, required_columns,
                                    category, owner, is_active, verified_at, created_at, updated_at)
            VALUES
              (:ns,'contract_value_vat','VAT Amount',
               'Value-added tax amount on contract',
               'NVL(VAT,0)',
               '["Contract"]','["VAT"]',
               'contracts','system', true, NOW(), NOW(), NOW())
            ON CONFLICT (namespace, metric_key, version) DO NOTHING;
            """
        )
        metric_gross = text(
            """
            INSERT INTO mem_metrics(namespace, metric_key, metric_name, description,
                                    calculation_sql, required_tables, required_columns,
                                    category, owner, is_active, verified_at, created_at, updated_at)
            VALUES
              (:ns,'contract_value_gross','Contract Value (Gross)',
               'Net + VAT',
               'NVL(CONTRACT_VALUE_NET_OF_VAT,0) + NVL(VAT,0)',
               '["Contract"]','["CONTRACT_VALUE_NET_OF_VAT","VAT"]',
               'contracts','system', true, NOW(), NOW(), NOW())
            ON CONFLICT (namespace, metric_key, version) DO NOTHING;
            """
        )

        snippet_sql = text(
            """
            INSERT INTO mem_snippets(namespace, title, description, sql_template, input_tables, output_columns, tags, created_at, updated_at, is_verified)
            VALUES (
              :ns,
              'contract_stakeholders_rows',
              'Unroll CONTRACT_STAKEHOLDER_[1..8] + DEPARTMENT_[1..8] into rows (no DB view).',
              :tpl,
              '["Contract"]',
              '["DWDOCID","CONTRACT_ID","CONTRACT_OWNER","OWNER_DEPARTMENT","CONTRACT_VALUE_NET_OF_VAT","VAT","CONTRACT_VALUE_GROSS","START_DATE","END_DATE","REQUEST_DATE","CONTRACT_STATUS","REQUEST_TYPE","ENTITY_NO","DEPARTMENT_OUL","SLOT","STAKEHOLDER","DEPARTMENT"]',
              '["dw","contracts","stakeholders","unnest"]',
              NOW(), NOW(), true
            )
            ON CONFLICT DO NOTHING;
            """
        )

        template_sql = """
SELECT
  DWDOCID, CONTRACT_ID, CONTRACT_OWNER, OWNER_DEPARTMENT,
  CONTRACT_VALUE_NET_OF_VAT, VAT,
  NVL(CONTRACT_VALUE_NET_OF_VAT,0) + NVL(VAT,0) AS CONTRACT_VALUE_GROSS,
  START_DATE, END_DATE, REQUEST_DATE, CONTRACT_STATUS, REQUEST_TYPE,
  ENTITY_NO, DEPARTMENT_OUL,
  '1' AS SLOT, CONTRACT_STAKEHOLDER_1 AS STAKEHOLDER, DEPARTMENT_1 AS DEPARTMENT
FROM Contract
UNION ALL
SELECT DWDOCID, CONTRACT_ID, CONTRACT_OWNER, OWNER_DEPARTMENT,
       CONTRACT_VALUE_NET_OF_VAT, VAT,
       NVL(CONTRACT_VALUE_NET_OF_VAT,0) + NVL(VAT,0),
       START_DATE, END_DATE, REQUEST_DATE, CONTRACT_STATUS, REQUEST_TYPE,
       ENTITY_NO, DEPARTMENT_OUL,
       '2', CONTRACT_STAKEHOLDER_2, DEPARTMENT_2
FROM Contract
UNION ALL
SELECT DWDOCID, CONTRACT_ID, CONTRACT_OWNER, OWNER_DEPARTMENT,
       CONTRACT_VALUE_NET_OF_VAT, VAT,
       NVL(CONTRACT_VALUE_NET_OF_VAT,0) + NVL(VAT,0),
       START_DATE, END_DATE, REQUEST_DATE, CONTRACT_STATUS, REQUEST_TYPE,
       ENTITY_NO, DEPARTMENT_OUL,
       '3', CONTRACT_STAKEHOLDER_3, DEPARTMENT_3
FROM Contract
UNION ALL
SELECT DWDOCID, CONTRACT_ID, CONTRACT_OWNER, OWNER_DEPARTMENT,
       CONTRACT_VALUE_NET_OF_VAT, VAT,
       NVL(CONTRACT_VALUE_NET_OF_VAT,0) + NVL(VAT,0),
       START_DATE, END_DATE, REQUEST_DATE, CONTRACT_STATUS, REQUEST_TYPE,
       ENTITY_NO, DEPARTMENT_OUL,
       '4', CONTRACT_STAKEHOLDER_4, DEPARTMENT_4
FROM Contract
UNION ALL
SELECT DWDOCID, CONTRACT_ID, CONTRACT_OWNER, OWNER_DEPARTMENT,
       CONTRACT_VALUE_NET_OF_VAT, VAT,
       NVL(CONTRACT_VALUE_NET_OF_VAT,0) + NVL(VAT,0),
       START_DATE, END_DATE, REQUEST_DATE, CONTRACT_STATUS, REQUEST_TYPE,
       ENTITY_NO, DEPARTMENT_OUL,
       '5', CONTRACT_STAKEHOLDER_5, DEPARTMENT_5
FROM Contract
UNION ALL
SELECT DWDOCID, CONTRACT_ID, CONTRACT_OWNER, OWNER_DEPARTMENT,
       CONTRACT_VALUE_NET_OF_VAT, VAT,
       NVL(CONTRACT_VALUE_NET_OF_VAT,0) + NVL(VAT,0),
       START_DATE, END_DATE, REQUEST_DATE, CONTRACT_STATUS, REQUEST_TYPE,
       ENTITY_NO, DEPARTMENT_OUL,
       '6', CONTRACT_STAKEHOLDER_6, DEPARTMENT_6
FROM Contract
UNION ALL
SELECT DWDOCID, CONTRACT_ID, CONTRACT_OWNER, OWNER_DEPARTMENT,
       CONTRACT_VALUE_NET_OF_VAT, VAT,
       NVL(CONTRACT_VALUE_NET_OF_VAT,0) + NVL(VAT,0),
       START_DATE, END_DATE, REQUEST_DATE, CONTRACT_STATUS, REQUEST_TYPE,
       ENTITY_NO, DEPARTMENT_OUL,
       '7', CONTRACT_STAKEHOLDER_7, DEPARTMENT_7
FROM Contract
UNION ALL
SELECT DWDOCID, CONTRACT_ID, CONTRACT_OWNER, OWNER_DEPARTMENT,
       CONTRACT_VALUE_NET_OF_VAT, VAT,
       NVL(CONTRACT_VALUE_NET_OF_VAT,0) + NVL(VAT,0),
       START_DATE, END_DATE, REQUEST_DATE, CONTRACT_STATUS, REQUEST_TYPE,
       ENTITY_NO, DEPARTMENT_OUL,
       '8', CONTRACT_STAKEHOLDER_8, DEPARTMENT_8
FROM Contract
""".strip()

        with mem.begin() as conn:
            conn.execute(metric_net, {"ns": namespace})
            conn.execute(metric_vat, {"ns": namespace})
            conn.execute(metric_gross, {"ns": namespace})
            conn.execute(snippet_sql, {"ns": namespace, "tpl": template_sql})

        return jsonify(ok=True, namespace=namespace, metrics=3, snippets=1)

    @bp.post("/answer")
    def answer():
        payload = request.get_json(force=True)
        question = (payload.get("question") or "").strip().lower()

        if not question:
            return jsonify(status="needs_clarification", questions=["Provide a question to answer."]), 200

        if "expir" in question and "day" in question:
            match = re.search(r"next\s+(\d+)\s*day", question)
            days = int(match.group(1)) if match else 30
            sql = text(
                """
                SELECT CONTRACT_ID,
                       CONTRACT_OWNER,
                       OWNER_DEPARTMENT,
                       END_DATE,
                       NVL(CONTRACT_VALUE_NET_OF_VAT,0) + NVL(VAT,0) AS CONTRACT_VALUE_GROSS
                  FROM Contract
                 WHERE END_DATE BETWEEN TRUNC(SYSDATE) AND TRUNC(SYSDATE) + :days
                 ORDER BY END_DATE ASC
                """
            )
            engine = _dw_engine()
            with engine.begin() as conn:
                rows = [dict(r) for r in conn.execute(sql, {"days": days}).mappings().all()]
            return jsonify(status="answered", rows=rows, sql=str(sql)), 200

        if "top" in question and "stakeholder" in question:
            sql = text(
                """
                SELECT STAKEHOLDER,
                       SUM(NVL(CONTRACT_VALUE_NET_OF_VAT,0) + NVL(VAT,0)) AS GROSS_TOTAL
                FROM (
                    SELECT NVL(CONTRACT_VALUE_NET_OF_VAT,0) AS NETV, NVL(VAT,0) AS VAT,
                           CONTRACT_STAKEHOLDER_1 AS STAKEHOLDER FROM Contract
                    UNION ALL SELECT NVL(CONTRACT_VALUE_NET_OF_VAT,0), NVL(VAT,0), CONTRACT_STAKEHOLDER_2 FROM Contract
                    UNION ALL SELECT NVL(CONTRACT_VALUE_NET_OF_VAT,0), NVL(VAT,0), CONTRACT_STAKEHOLDER_3 FROM Contract
                    UNION ALL SELECT NVL(CONTRACT_VALUE_NET_OF_VAT,0), NVL(VAT,0), CONTRACT_STAKEHOLDER_4 FROM Contract
                    UNION ALL SELECT NVL(CONTRACT_VALUE_NET_OF_VAT,0), NVL(VAT,0), CONTRACT_STAKEHOLDER_5 FROM Contract
                    UNION ALL SELECT NVL(CONTRACT_VALUE_NET_OF_VAT,0), NVL(VAT,0), CONTRACT_STAKEHOLDER_6 FROM Contract
                    UNION ALL SELECT NVL(CONTRACT_VALUE_NET_OF_VAT,0), NVL(VAT,0), CONTRACT_STAKEHOLDER_7 FROM Contract
                    UNION ALL SELECT NVL(CONTRACT_VALUE_NET_OF_VAT,0), NVL(VAT,0), CONTRACT_STAKEHOLDER_8 FROM Contract
                )
                WHERE STAKEHOLDER IS NOT NULL
                GROUP BY STAKEHOLDER
                ORDER BY GROSS_TOTAL DESC
                FETCH FIRST 10 ROWS ONLY
                """
            )
            engine = _dw_engine()
            with engine.begin() as conn:
                rows = [dict(r) for r in conn.execute(sql).mappings().all()]
            return jsonify(status="answered", rows=rows, sql=str(sql)), 200

        return (
            jsonify(
                status="needs_clarification",
                questions=[
                    "Which time window or filter (e.g., next 30 days, this quarter)?",
                    "Aggregate by what (stakeholder, owner_department, entity_no)?",
                    "Return which columns?",
                ],
            ),
            200,
        )

    return bp
