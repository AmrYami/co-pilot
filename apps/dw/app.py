from flask import Blueprint, current_app, jsonify, request
from core.settings import Settings
from core.sql_exec import get_mem_engine
from sqlalchemy import text
import json


def create_dw_blueprint(settings: Settings) -> Blueprint:
    """
    DW blueprint factory. We avoid any FA imports here.
    """
    bp = Blueprint("dw", __name__, url_prefix="/dw")

    @bp.route("/seed", methods=["POST"])
    def seed():
        """
        Seed minimal knowledge for table Contract into memory DB (mem_* tables).
        No Oracle views. Only mem_* tables are touched.
        """
        payload = request.get_json(silent=True) or {}
        ns = payload.get("namespace") or "dw::common"
        force = bool(payload.get("force", False))

        mem = get_mem_engine(settings)

        # 1) Basic mappings / glossary for Contract (stakeholders & departments)
        #    This helps the planner recognize common terms.
        mappings = [
            # alias, canonical, mapping_type, scope
            ("stakeholder", "contract_stakeholder", "term", "global"),
            ("department",  "department",           "term", "global"),
            ("owner",       "contract_owner",       "term", "global"),
            ("value",       "contract_value_gross", "metric","global"),
            ("net value",   "contract_value_net",   "metric","global"),
            ("vat",         "vat",                  "term", "global"),
        ]

        metrics = [
            # metric_key, metric_name, calculation_sql, required_tables, required_columns, description
            ("contract_value_gross",
             "Contract Value (Gross)",
             # NVL for Oracle
             "NVL(CONTRACT_VALUE_NET_OF_VAT,0) + NVL(VAT,0)",
             ["Contract"],
             ["CONTRACT_VALUE_NET_OF_VAT", "VAT"],
             "Gross value = net + VAT"),

            ("active_contracts_count",
             "Active Contracts Count",
             "COUNT(*) FILTER (WHERE END_DATE IS NULL OR END_DATE >= SYSDATE)",
             ["Contract"],
             ["END_DATE"],
             "Number of contracts not yet expired"),
        ]

        # Snippet: unpivot stakeholders ↔ departments without creating a view (UNION ALL)
        unpivot_sql = """
        SELECT DWDOCID, CONTRACT_ID, CONTRACT_OWNER, OWNER_DEPARTMENT,
               CONTRACT_VALUE_NET_OF_VAT, VAT,
               NVL(CONTRACT_VALUE_NET_OF_VAT,0) + NVL(VAT,0) AS CONTRACT_VALUE_GROSS,
               START_DATE, END_DATE, REQUEST_DATE, CONTRACT_STATUS, REQUEST_TYPE,
               ENTITY_NO, DEPARTMENT_OUL, SLOT, STAKEHOLDER, DEPARTMENT
        FROM (
          SELECT DWDOCID, CONTRACT_ID, CONTRACT_OWNER, OWNER_DEPARTMENT,
                 CONTRACT_VALUE_NET_OF_VAT, VAT,
                 START_DATE, END_DATE, REQUEST_DATE, CONTRACT_STATUS, REQUEST_TYPE,
                 ENTITY_NO, DEPARTMENT_OUL,
                 '1' AS SLOT, CONTRACT_STAKEHOLDER_1 AS STAKEHOLDER, DEPARTMENT_1 AS DEPARTMENT FROM Contract
          UNION ALL
          SELECT DWDOCID, CONTRACT_ID, CONTRACT_OWNER, OWNER_DEPARTMENT,
                 CONTRACT_VALUE_NET_OF_VAT, VAT,
                 START_DATE, END_DATE, REQUEST_DATE, CONTRACT_STATUS, REQUEST_TYPE,
                 ENTITY_NO, DEPARTMENT_OUL,
                 '2', CONTRACT_STAKEHOLDER_2, DEPARTMENT_2 FROM Contract
          UNION ALL
          SELECT DWDOCID, CONTRACT_ID, CONTRACT_OWNER, OWNER_DEPARTMENT,
                 CONTRACT_VALUE_NET_OF_VAT, VAT,
                 START_DATE, END_DATE, REQUEST_DATE, CONTRACT_STATUS, REQUEST_TYPE,
                 ENTITY_NO, DEPARTMENT_OUL,
                 '3', CONTRACT_STAKEHOLDER_3, DEPARTMENT_3 FROM Contract
          UNION ALL
          SELECT DWDOCID, CONTRACT_ID, CONTRACT_OWNER, OWNER_DEPARTMENT,
                 CONTRACT_VALUE_NET_OF_VAT, VAT,
                 START_DATE, END_DATE, REQUEST_DATE, CONTRACT_STATUS, REQUEST_TYPE,
                 ENTITY_NO, DEPARTMENT_OUL,
                 '4', CONTRACT_STAKEHOLDER_4, DEPARTMENT_4 FROM Contract
          UNION ALL
          SELECT DWDOCID, CONTRACT_ID, CONTRACT_OWNER, OWNER_DEPARTMENT,
                 CONTRACT_VALUE_NET_OF_VAT, VAT,
                 START_DATE, END_DATE, REQUEST_DATE, CONTRACT_STATUS, REQUEST_TYPE,
                 ENTITY_NO, DEPARTMENT_OUL,
                 '5', CONTRACT_STAKEHOLDER_5, DEPARTMENT_5 FROM Contract
          UNION ALL
          SELECT DWDOCID, CONTRACT_ID, CONTRACT_OWNER, OWNER_DEPARTMENT,
                 CONTRACT_VALUE_NET_OF_VAT, VAT,
                 START_DATE, END_DATE, REQUEST_DATE, CONTRACT_STATUS, REQUEST_TYPE,
                 ENTITY_NO, DEPARTMENT_OUL,
                 '6', CONTRACT_STAKEHOLDER_6, DEPARTMENT_6 FROM Contract
          UNION ALL
          SELECT DWDOCID, CONTRACT_ID, CONTRACT_OWNER, OWNER_DEPARTMENT,
                 CONTRACT_VALUE_NET_OF_VAT, VAT,
                 START_DATE, END_DATE, REQUEST_DATE, CONTRACT_STATUS, REQUEST_TYPE,
                 ENTITY_NO, DEPARTMENT_OUL,
                 '7', CONTRACT_STAKEHOLDER_7, DEPARTMENT_7 FROM Contract
          UNION ALL
          SELECT DWDOCID, CONTRACT_ID, CONTRACT_OWNER, OWNER_DEPARTMENT,
                 CONTRACT_VALUE_NET_OF_VAT, VAT,
                 START_DATE, END_DATE, REQUEST_DATE, CONTRACT_STATUS, REQUEST_TYPE,
                 ENTITY_NO, DEPARTMENT_OUL,
                 '8', CONTRACT_STAKEHOLDER_8, DEPARTMENT_8 FROM Contract
        )
        """

        with mem.begin() as c:
            if force:
                c.execute(text("DELETE FROM mem_mappings WHERE namespace=:ns"), {"ns": ns})
                c.execute(text("DELETE FROM mem_metrics  WHERE namespace=:ns"), {"ns": ns})
                c.execute(text("DELETE FROM mem_snippets WHERE namespace=:ns"), {"ns": ns})

            # Upsert mappings
            for alias, canonical, mtype, scope in mappings:
                c.execute(text("""
                    INSERT INTO mem_mappings(namespace, alias, canonical, mapping_type, scope, source, confidence)
                    VALUES (:ns, :alias, :canonical, :mtype, :scope, 'seed', 0.95)
                    ON CONFLICT (namespace, alias, mapping_type, scope) DO UPDATE
                      SET canonical = EXCLUDED.canonical,
                          updated_at = NOW()
                """), dict(ns=ns, alias=alias, canonical=canonical, mtype=mtype, scope=scope))

            # Upsert metrics
            for key, name, sql_expr, req_tables, req_cols, desc in metrics:
                c.execute(text("""
                    INSERT INTO mem_metrics(namespace, metric_key, metric_name, description,
                                            calculation_sql, required_tables, required_columns, category, owner, is_active)
                    VALUES(:ns, :key, :name, :desc, :calc, :rt::jsonb, :rc::jsonb, 'contracts','dw', true)
                    ON CONFLICT (namespace, metric_key, version) DO UPDATE
                      SET calculation_sql = EXCLUDED.calculation_sql,
                          description      = EXCLUDED.description,
                          updated_at       = NOW()
                """), dict(
                    ns=ns, key=key, name=name, desc=desc, calc=sql_expr,
                    rt=json.dumps(req_tables), rc=json.dumps(req_cols)
                ))

            # Saved snippet for unpivot
            c.execute(text("""
                INSERT INTO mem_snippets(namespace, title, description, sql_template, input_tables, tags, is_verified, verified_by)
                VALUES(:ns, 'dw_contract_stakeholders_unpivot',
                       'UNION ALL unpivot of 8 stakeholder/department pairs (no DB views).',
                       :sql, '["Contract"]'::jsonb, '["dw","contracts","unpivot"]'::jsonb, true, 'seed')
                ON CONFLICT DO NOTHING
            """), dict(ns=ns, sql=unpivot_sql))

        return jsonify({"ok": True, "namespace": ns, "seeded": {"mappings": len(mappings), "metrics": len(metrics), "snippets": 1}})

    @bp.route("/answer", methods=["POST"])
    def answer():
        """
        Hand the question to Pipeline.answer using the DW namespace.
        """
        payload = request.get_json(force=True) or {}
        question  = payload.get("question") or ""
        auth_email = payload.get("auth_email")
        prefixes   = payload.get("prefixes") or []   # keep shape consistent

        # Use the pipeline created in main app factory
        pipeline = current_app.config.get("pipeline")
        if not pipeline:
            return jsonify({"ok": False, "error": "Pipeline not available"}), 500

        try:
            result = pipeline.answer(
                question=question,
                auth_email=auth_email,
                prefixes=prefixes,
                datasource="docuware",
                namespace="dw::common",
            )
        except NotImplementedError as exc:  # pragma: no cover - legacy pipeline stub
            return jsonify({"ok": False, "error": str(exc)}), 501

        return jsonify(result)

    @bp.route("/metrics", methods=["GET"])
    def metrics():
        ns = request.args.get("namespace") or "dw::common"
        mem = get_mem_engine(settings)
        rows = mem.execute(text("""
            SELECT metric_key, metric_name, calculation_sql, description
              FROM mem_metrics
             WHERE namespace = :ns AND is_active = true
             ORDER BY metric_key
        """), {"ns": ns}).mappings().all()
        return jsonify({"ok": True, "namespace": ns, "metrics": rows})

    return bp
