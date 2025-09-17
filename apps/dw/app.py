from __future__ import annotations

import json
from typing import Any, Dict, List

from flask import Blueprint, jsonify, request
from sqlalchemy import inspect, text

from core.datasources import DatasourceRegistry
from core.settings import Settings

from .answerer import AnswerError, StakeholderAnswerer


NAMESPACE = "dw::common"
dw_bp = Blueprint("dw", __name__)


def _infer_value_type(value):
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int) and not isinstance(value, bool):
        return "int"
    if isinstance(value, float):
        return "float"
    if isinstance(value, (list, dict)):
        return "json"
    return "string"


def _manual_upsert_setting(
    conn,
    *,
    key: str,
    value,
    value_type: str | None = None,
    scope: str = "namespace",
    scope_id=None,
    updated_by: str = "dw",
    is_secret: bool = False,
):
    vtype = value_type or _infer_value_type(value)
    value_json = json.dumps(value, ensure_ascii=False)
    update_stmt = text(
        """
        UPDATE mem_settings
           SET value = CAST(:val AS jsonb),
               value_type = :vtype,
               updated_by = :upd_by,
               updated_at = NOW(),
               is_secret  = :secret
         WHERE namespace = :ns
           AND key       = :key
           AND scope     = :scope
           AND ((:scope_id IS NULL AND scope_id IS NULL) OR scope_id = :scope_id)
        """
    )
    result = conn.execute(
        update_stmt,
        {
            "ns": NAMESPACE,
            "key": key,
            "val": value_json,
            "vtype": vtype,
            "scope": scope,
            "scope_id": scope_id,
            "upd_by": updated_by,
            "secret": is_secret,
        },
    )
    if result.rowcount and result.rowcount > 0:
        return

    insert_stmt = text(
        """
        INSERT INTO mem_settings(namespace, key, value, value_type, scope, scope_id,
                                 overridable, updated_by, created_at, updated_at, is_secret)
        VALUES (:ns, :key, CAST(:val AS jsonb), :vtype, :scope, :scope_id,
                true, :upd_by, NOW(), NOW(), :secret)
        """
    )
    conn.execute(
        insert_stmt,
        {
            "ns": NAMESPACE,
            "key": key,
            "val": value_json,
            "vtype": vtype,
            "scope": scope,
            "scope_id": scope_id,
            "upd_by": updated_by,
            "secret": is_secret,
        },
    )


def _ensure_mem_snapshot_schema(mem_engine) -> None:
    with mem_engine.begin() as conn:
        conn.execute(
            text(
                """
                ALTER TABLE mem_snapshots
                ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ
                """
            )
        )
        conn.execute(
            text(
                """
                UPDATE mem_snapshots
                   SET updated_at = COALESCE(updated_at, created_at)
                 WHERE updated_at IS NULL
                """
            )
        )


def _seed_semantic_layer(mem_engine) -> Dict[str, List[str]]:
    """Seed baseline metrics and mappings required for DW answering."""

    required_tables = ["Contract"]
    required_columns = [
        "CONTRACT_VALUE_NET_OF_VAT",
        "VAT",
        "START_DATE",
        "END_DATE",
        "REQUEST_DATE",
        "CONTRACT_STAKEHOLDER_1",
        "DEPARTMENT_1",
        "CONTRACT_STAKEHOLDER_2",
        "DEPARTMENT_2",
        "CONTRACT_STAKEHOLDER_3",
        "DEPARTMENT_3",
        "CONTRACT_STAKEHOLDER_4",
        "DEPARTMENT_4",
        "CONTRACT_STAKEHOLDER_5",
        "DEPARTMENT_5",
        "CONTRACT_STAKEHOLDER_6",
        "DEPARTMENT_6",
        "CONTRACT_STAKEHOLDER_7",
        "DEPARTMENT_7",
        "CONTRACT_STAKEHOLDER_8",
        "DEPARTMENT_8",
    ]

    payload = {
        "ns": NAMESPACE,
        "key": "contract_value_gross",
        "name": "Contract Value (Gross)",
        "desc": "Gross value = net + VAT",
        "calc": "NVL(CONTRACT_VALUE_NET_OF_VAT,0) + NVL(VAT,0)",
        "rt": json.dumps(required_tables),
        "rc": json.dumps(required_columns),
    }
    metric_sql = text(
        """
        INSERT INTO mem_metrics(namespace, metric_key, metric_name, description,
                                calculation_sql, required_tables, required_columns,
                                category, owner, is_active)
        VALUES(:ns, :key, :name, :desc, :calc,
               CAST(:rt AS jsonb), CAST(:rc AS jsonb),
               'contracts','dw', true)
        ON CONFLICT (namespace, metric_key, version) DO UPDATE
          SET calculation_sql = EXCLUDED.calculation_sql,
              description      = EXCLUDED.description,
              updated_at       = NOW()
        """
    )

    seeded = {"metrics": [], "mappings": []}

    with mem_engine.begin() as conn:
        conn.execute(metric_sql, payload)
        seeded["metrics"].append(payload["key"])

        for slot in range(1, 9):
            conn.execute(
                text(
                    """
                    INSERT INTO mem_mappings(namespace, alias, canonical, mapping_type, scope, source, confidence)
                    VALUES (:ns, :alias, :canonical, 'column', 'global', 'dw_seed', 0.98)
                    ON CONFLICT (namespace, alias, mapping_type, scope) DO UPDATE
                      SET canonical = EXCLUDED.canonical,
                          confidence = EXCLUDED.confidence,
                          updated_at = NOW()
                    """
                ),
                {
                    "ns": NAMESPACE,
                    "alias": f"CONTRACT_STAKEHOLDER_{slot}",
                    "canonical": "stakeholder",
                },
            )
            conn.execute(
                text(
                    """
                    INSERT INTO mem_mappings(namespace, alias, canonical, mapping_type, scope, source, confidence)
                    VALUES (:ns, :alias, :canonical, 'column', 'global', 'dw_seed', 0.95)
                    ON CONFLICT (namespace, alias, mapping_type, scope) DO UPDATE
                      SET canonical = EXCLUDED.canonical,
                          confidence = EXCLUDED.confidence,
                          updated_at = NOW()
                    """
                ),
                {
                    "ns": NAMESPACE,
                    "alias": f"DEPARTMENT_{slot}",
                    "canonical": "department",
                },
            )

        for alias in ("stakeholder", "stakeholders"):
            conn.execute(
                text(
                    """
                    INSERT INTO mem_mappings(namespace, alias, canonical, mapping_type, scope, source, confidence)
                    VALUES (:ns, :alias, 'stakeholder', 'term', 'global', 'dw_seed', 0.99)
                    ON CONFLICT (namespace, alias, mapping_type, scope) DO UPDATE
                      SET canonical = EXCLUDED.canonical,
                          confidence = EXCLUDED.confidence,
                          updated_at = NOW()
                    """
                ),
                {"ns": NAMESPACE, "alias": alias},
            )

        for alias in ("department", "departments"):
            conn.execute(
                text(
                    """
                    INSERT INTO mem_mappings(namespace, alias, canonical, mapping_type, scope, source, confidence)
                    VALUES (:ns, :alias, 'department', 'term', 'global', 'dw_seed', 0.95)
                    ON CONFLICT (namespace, alias, mapping_type, scope) DO UPDATE
                      SET canonical = EXCLUDED.canonical,
                          confidence = EXCLUDED.confidence,
                          updated_at = NOW()
                    """
                ),
                {"ns": NAMESPACE, "alias": alias},
            )

        seeded["mappings"].extend(
            [
                "stakeholder_columns",
                "department_columns",
                "stakeholder_terms",
                "department_terms",
            ]
        )

    return seeded


@dw_bp.route("/ingest", methods=["POST"])
def ingest():
    settings = Settings(namespace=NAMESPACE)
    mem_engine = settings.mem_engine()
    registry = DatasourceRegistry(settings, namespace=NAMESPACE)
    engine = registry.engine(None)

    inspector = inspect(engine)
    table_lookup = {name.upper(): name for name in inspector.get_table_names()}
    if "CONTRACT" not in table_lookup:
        return jsonify({"ok": False, "error": "Contract table not found in datasource."}), 400

    actual_name = table_lookup["CONTRACT"]
    columns = inspector.get_columns(actual_name)

    _ensure_mem_snapshot_schema(mem_engine)

    with mem_engine.begin() as conn:
        snapshot_id = conn.execute(
            text(
                """
                INSERT INTO mem_snapshots(namespace, schema_hash)
                VALUES (:ns, :hash)
                ON CONFLICT (namespace, schema_hash) DO UPDATE SET updated_at = NOW()
                RETURNING id
                """
            ),
            {"ns": NAMESPACE, "hash": "dw-oracle-contract-v1"},
        ).scalar_one()

        table_id = conn.execute(
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
                "sid": snapshot_id,
                "tname": actual_name,
                "sname": None,
                "comment": "DocuWare Contract base table",
            },
        ).scalar_one()

        for column in columns:
            conn.execute(
                text(
                    """
                    INSERT INTO mem_columns(namespace, table_id, column_name, data_type, is_nullable)
                    VALUES (:ns, :tid, :cname, :ctype, :nullable)
                    ON CONFLICT (namespace, table_id, column_name)
                    DO UPDATE SET data_type = EXCLUDED.data_type, updated_at = NOW()
                    """
                ),
                {
                    "ns": NAMESPACE,
                    "tid": table_id,
                    "cname": column.get("name"),
                    "ctype": str(column.get("type")),
                    "nullable": bool(column.get("nullable", True)),
                },
            )

        _manual_upsert_setting(
            conn,
            key="DW_CONTRACT_TABLE",
            value=actual_name,
            updated_by="dw_ingest",
        )
        _manual_upsert_setting(
            conn,
            key="DEFAULT_DATASOURCE",
            value="docuware",
            updated_by="dw_ingest",
        )

    seeded = _seed_semantic_layer(mem_engine)

    return jsonify(
        {
            "ok": True,
            "namespace": NAMESPACE,
            "table": actual_name,
            "columns": len(columns),
            "seeded": seeded,
        }
    )


@dw_bp.route("/seed", methods=["POST"])
def seed():
    settings = Settings(namespace=NAMESPACE)
    mem_engine = settings.mem_engine()
    seeded = _seed_semantic_layer(mem_engine)
    return jsonify({"ok": True, "namespace": NAMESPACE, "seeded": seeded})


@dw_bp.route("/metrics", methods=["GET"])
def metrics():
    settings = Settings(namespace=NAMESPACE)
    mem_engine = settings.mem_engine()
    with mem_engine.begin() as conn:
        rows = conn.execute(
            text(
                """
                SELECT metric_key, metric_name, description, category, is_active, updated_at
                  FROM mem_metrics
                 WHERE namespace = :ns
              ORDER BY metric_key
                """
            ),
            {"ns": NAMESPACE},
        ).mappings().all()
    return jsonify(
        {
            "ok": True,
            "namespace": NAMESPACE,
            "metrics": [dict(row) for row in rows],
        }
    )


@dw_bp.route("/answer", methods=["POST"])
def answer():
    payload = request.get_json(force=True) or {}
    question = str(payload.get("question") or "").strip()
    if not question:
        return jsonify({"ok": False, "error": "Question text is required."}), 400

    settings = Settings(namespace=NAMESPACE)
    mem = settings.mem_engine()
    registry = DatasourceRegistry(settings, namespace=NAMESPACE)
    answerer = StakeholderAnswerer(settings, mem, registry)

    try:
        result = answerer.answer(question)
    except AnswerError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400

    response: Dict[str, Any] = {
        "ok": True,
        "sql": result.sql,
        "rows": result.rows,
        "meta": {
            "top_n": result.top_n,
            "date_start": result.date_start.isoformat(),
            "date_end": result.date_end.isoformat(),
            "tags": result.tags,
        },
    }
    if not result.rows:
        response["hint"] = (
            "No results for last month. Try a wider window (e.g., last 90 days) or "
            "use START_DATE/END_DATE filters."
        )

    if result.run_id is not None:
        response["run_id"] = result.run_id

    return jsonify(response)
