from __future__ import annotations

import json

from flask import Blueprint, request
from sqlalchemy import inspect, text

from core.datasources import DatasourceRegistry
from core.settings import Settings
from core.sql_exec import get_mem_engine


dw_bp = Blueprint("dw", __name__)

NAMESPACE = "dw::common"
TABLE = '"Contract"'


# ---------------------------------------------------------------------------
def _dw_engine(settings: Settings):
    return DatasourceRegistry(settings, namespace=NAMESPACE).engine(None)


def _last_month_bounds_sql():
    return (
        "TRUNC(ADD_MONTHS(SYSDATE, -1), 'MM')",
        "TRUNC(SYSDATE, 'MM')",
    )


def _stakeholder_union_sql():
    unions = []
    for i in range(1, 9):
        unions.append(
            f"""
            SELECT
              CONTRACT_ID,
              COALESCE(REQUEST_DATE, START_DATE, END_DATE) AS REF_DATE,
              NVL(CONTRACT_VALUE_NET_OF_VAT,0) + NVL(VAT,0) AS VALUE_GROSS,
              CONTRACT_OWNER,
              OWNER_DEPARTMENT,
              '{i}' AS SLOT,
              CONTRACT_STAKEHOLDER_{i} AS STAKEHOLDER,
              DEPARTMENT_{i} AS DEPARTMENT
            FROM {TABLE}
            """
        )
    return " \nUNION ALL\n".join(unions)


def _top10_stakeholders_sql():
    start_sql, end_sql = _last_month_bounds_sql()
    union_sql = _stakeholder_union_sql()
    return f"""
        WITH stakeholders AS (
            {union_sql}
        )
        SELECT
          TRIM(STAKEHOLDER) AS stakeholder,
          SUM(VALUE_GROSS) AS total_value_gross,
          COUNT(DISTINCT CONTRACT_ID) AS contract_count,
          LISTAGG(DISTINCT TRIM(DEPARTMENT), ', ') WITHIN GROUP (ORDER BY TRIM(DEPARTMENT)) AS departments
        FROM stakeholders
        WHERE STAKEHOLDER IS NOT NULL AND TRIM(STAKEHOLDER) <> ''
          AND REF_DATE >= {start_sql}
          AND REF_DATE <  {end_sql}
        GROUP BY TRIM(STAKEHOLDER)
        ORDER BY total_value_gross DESC
        FETCH FIRST 10 ROWS ONLY
    """


def _store_snippet(mem, sql_text: str, tags: list[str]) -> None:
    with mem.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO mem_snippets(namespace, title, description, sql_template, sql_raw,
                                         input_tables, output_columns, tags, is_verified, source)
                VALUES (:ns, :title, :desc, :tmpl, :raw, :in_tbls, :out_cols, :tags, true, 'dw')
                """
            ),
            {
                "ns": NAMESPACE,
                "title": "Top 10 stakeholders by gross value (last month)",
                "desc": "Union 8 stakeholder slots; sum gross value; last month.",
                "tmpl": sql_text,
                "raw": sql_text,
                "in_tbls": json.dumps([{"table": "Contract"}]),
                "out_cols": json.dumps([
                    "stakeholder",
                    "total_value_gross",
                    "contract_count",
                    "departments",
                ]),
                "tags": json.dumps(tags),
            },
        )


def _seed_mappings_and_metrics(mem) -> None:
    required_tables = [{"table": "Contract"}]
    required_columns = ["CONTRACT_VALUE_NET_OF_VAT", "VAT"]
    with mem.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO mem_metrics(namespace, metric_key, metric_name, description,
                                        calculation_sql, required_tables, required_columns,
                                        category, owner, is_active)
                VALUES (:ns, :key, :name, :desc, :calc, :rt, :rc, 'contracts', 'dw', true)
                ON CONFLICT (namespace, metric_key, version) DO UPDATE
                  SET calculation_sql = EXCLUDED.calculation_sql,
                      description      = EXCLUDED.description,
                      updated_at       = NOW()
                """
            ),
            {
                "ns": NAMESPACE,
                "key": "contract_value_gross",
                "name": "Contract Value (Gross)",
                "desc": "Gross value = NVL(CONTRACT_VALUE_NET_OF_VAT,0) + NVL(VAT,0)",
                "calc": "NVL(CONTRACT_VALUE_NET_OF_VAT,0) + NVL(VAT,0)",
                "rt": json.dumps(required_tables),
                "rc": json.dumps(required_columns),
            },
        )

        for i in range(1, 9):
            conn.execute(
                text(
                    """
                    INSERT INTO mem_mappings(namespace, alias, canonical, mapping_type, scope, source, confidence)
                    VALUES (:ns, :alias, :canon, 'column', 'global', 'dw_seed', 0.95)
                    ON CONFLICT (namespace, alias, mapping_type, scope) DO UPDATE
                      SET canonical = EXCLUDED.canonical,
                          confidence = EXCLUDED.confidence,
                          updated_at = NOW()
                    """
                ),
                {
                    "ns": NAMESPACE,
                    "alias": f"CONTRACT_STAKEHOLDER_{i}",
                    "canon": "stakeholder",
                },
            )

        for alias in ("stakeholder", "stakeholders"):
            conn.execute(
                text(
                    """
                    INSERT INTO mem_mappings(namespace, alias, canonical, mapping_type, scope, source, confidence)
                    VALUES (:ns, :alias, 'stakeholder', 'term', 'global', 'dw_seed', 0.98)
                    ON CONFLICT (namespace, alias, mapping_type, scope) DO UPDATE
                      SET canonical = EXCLUDED.canonical,
                          confidence = EXCLUDED.confidence,
                          updated_at = NOW()
                    """
                ),
                {"ns": NAMESPACE, "alias": alias},
            )


# ---------------------------------------------------------------------------
@dw_bp.route("/dw/ingest", methods=["POST"])
def ingest():
    settings = Settings(NAMESPACE)
    mem = get_mem_engine(settings)
    eng = _dw_engine(settings)

    inspector = inspect(eng)
    table_names = {t.upper(): t for t in inspector.get_table_names()}
    if "CONTRACT" not in table_names:
        return {"ok": False, "error": "Contract table not found in Oracle."}, 400

    actual_name = table_names["CONTRACT"]

    with mem.begin() as conn:
        snap_id = conn.execute(
            text(
                """
                INSERT INTO mem_snapshots(namespace, schema_hash)
                VALUES (:ns, :hash)
                ON CONFLICT (namespace, schema_hash) DO UPDATE SET updated_at = NOW()
                RETURNING id
                """
            ),
            {"ns": NAMESPACE, "hash": "dw-oracle-v1"},
        ).scalar_one()

        tbl_id = conn.execute(
            text(
                """
                INSERT INTO mem_tables(namespace, snapshot_id, table_name, schema_name, table_comment)
                VALUES (:ns, :sid, :tname, :sname, :comment)
                ON CONFLICT (namespace, table_name, schema_name)
                DO UPDATE SET snapshot_id = EXCLUDED.snapshot_id, updated_at = NOW()
                RETURNING id
                """
            ),
            {
                "ns": NAMESPACE,
                "sid": snap_id,
                "tname": actual_name,
                "sname": None,
                "comment": "DocuWare contracts",
            },
        ).scalar_one()

        columns = inspector.get_columns(actual_name)
        for col in columns:
            conn.execute(
                text(
                    """
                    INSERT INTO mem_columns(namespace, table_id, column_name, data_type, is_nullable)
                    VALUES (:ns, :tid, :cname, :dtype, :nullable)
                    ON CONFLICT (namespace, table_id, column_name)
                    DO UPDATE SET data_type = EXCLUDED.data_type, updated_at = NOW()
                    """
                ),
                {
                    "ns": NAMESPACE,
                    "tid": tbl_id,
                    "cname": col["name"],
                    "dtype": str(col.get("type")),
                    "nullable": bool(col.get("nullable", True)),
                },
            )

    return {"ok": True, "namespace": NAMESPACE, "ingested": actual_name}


@dw_bp.route("/dw/seed", methods=["POST"])
def seed():
    settings = Settings(NAMESPACE)
    mem = get_mem_engine(settings)
    _seed_mappings_and_metrics(mem)
    return {
        "ok": True,
        "namespace": NAMESPACE,
        "seeded": ["metrics:contract_value_gross", "mappings:stakeholder"],
    }


@dw_bp.route("/dw/metrics", methods=["GET"])
def metrics():
    settings = Settings(NAMESPACE)
    mem = get_mem_engine(settings)
    with mem.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT metric_key, metric_name, description, calculation_sql, category, is_active
                  FROM mem_metrics
                 WHERE namespace = :ns
              ORDER BY metric_key
                """
            ),
            {"ns": NAMESPACE},
        ).mappings().all()
    return {"ok": True, "metrics": [dict(r) for r in rows]}


@dw_bp.route("/dw/answer", methods=["POST"])
def answer():
    payload = request.get_json(force=True) or {}
    question = (payload.get("question") or "").lower().strip()

    is_top10 = (
        "stakeholder" in question
        and "top 10" in question
        and ("last month" in question or "previous month" in question)
        and ("value" in question or "gross" in question or "contract value" in question)
    )

    if not is_top10:
        return {
            "ok": False,
            "error": "Only the 'top 10 stakeholders by contract value last month' pattern is implemented right now.",
        }, 400

    settings = Settings(NAMESPACE)
    mem = get_mem_engine(settings)
    eng = _dw_engine(settings)

    sql_text = _top10_stakeholders_sql()

    with eng.connect() as conn:
        rows = conn.execute(text(sql_text)).mappings().all()

    _seed_mappings_and_metrics(mem)
    _store_snippet(mem, sql_text, tags=["dw", "contracts", "stakeholders", "top10", "last_month"])

    return {"ok": True, "sql": sql_text, "rows": [dict(r) for r in rows]}
