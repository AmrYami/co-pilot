from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, List

from flask import Blueprint, current_app, jsonify, request
from sqlalchemy import inspect as sqla_inspect, text
from sqlalchemy.exc import SQLAlchemyError

from core.datasources import DatasourceRegistry
from core.settings import Settings
from core.sql_exec import get_mem_engine


dw_bp = Blueprint("dw", __name__)


def _payload() -> Dict[str, Any]:
    return request.get_json(silent=True) or {}


def _namespace(payload: Dict[str, Any] | None = None) -> str:
    if payload and payload.get("namespace"):
        return str(payload["namespace"])
    arg_ns = request.args.get("namespace")
    if arg_ns:
        return arg_ns
    payload = payload or _payload()
    ns = payload.get("namespace")
    return str(ns) if ns else "dw::common"


@dw_bp.route("/dw/seed", methods=["POST"])
def seed() -> Any:
    payload = _payload()
    ns = _namespace(payload)

    settings = Settings(namespace=ns)
    mem = get_mem_engine(settings)

    required_tables = json.dumps(["Contract"])
    required_columns = json.dumps(["CONTRACT_VALUE_NET_OF_VAT", "VAT"])

    with mem.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO mem_metrics(
                    namespace, metric_key, metric_name, description,
                    calculation_sql, required_tables, required_columns,
                    category, owner, is_active
                )
                VALUES (
                    :ns, :key, :name, :desc,
                    :calc, CAST(:rt AS jsonb), CAST(:rc AS jsonb),
                    'contracts', 'dw', true
                )
                ON CONFLICT (namespace, metric_key, version) DO UPDATE
                SET calculation_sql = EXCLUDED.calculation_sql,
                    description      = EXCLUDED.description,
                    updated_at       = NOW()
                """
            ),
            {
                "ns": ns,
                "key": "contract_value_gross",
                "name": "Contract Value (Gross)",
                "desc": "Gross value = net + VAT",
                "calc": "NVL(CONTRACT_VALUE_NET_OF_VAT,0) + NVL(VAT,0)",
                "rt": required_tables,
                "rc": required_columns,
            },
        )

    return jsonify(
        {
            "ok": True,
            "namespace": ns,
            "seeded_metrics": ["contract_value_gross"],
        }
    )


@dw_bp.route("/dw/ingest", methods=["POST"])
def ingest() -> Any:
    payload = _payload()
    ns = _namespace(payload)
    tables: List[str] = payload.get("tables") or ["Contract"]

    settings = Settings(namespace=ns)
    mem = get_mem_engine(settings)

    ds_registry = DatasourceRegistry(settings, namespace=ns)
    oracle_engine = ds_registry.engine(None)
    inspector = sqla_inspect(oracle_engine)

    processed: List[str] = []
    errors: List[Dict[str, Any]] = []

    schema_signature = hashlib.sha256(
        f"{ns}::{'|'.join(sorted(tables))}".encode("utf-8")
    ).hexdigest()[:16]

    with mem.begin() as conn:
        snapshot_id = conn.execute(
            text(
                """
                INSERT INTO mem_snapshots(namespace, schema_hash, diff_from)
                VALUES (:ns, :hash, NULL)
                ON CONFLICT (namespace, schema_hash) DO UPDATE
                SET schema_hash = EXCLUDED.schema_hash
                RETURNING id
                """
            ),
            {"ns": ns, "hash": schema_signature},
        ).scalar_one()

        for table_name in tables:
            try:
                columns = inspector.get_columns(table_name)
                pk = inspector.get_pk_constraint(table_name) or {}
            except SQLAlchemyError as exc:  # pragma: no cover - inspection errors
                errors.append({"table": table_name, "error": str(exc)})
                continue

            pk_cols = pk.get("constrained_columns") or []

            table_row_id = conn.execute(
                text(
                    """
                    INSERT INTO mem_tables(
                        namespace, snapshot_id, table_name, schema_name,
                        row_count, size_bytes, primary_key, engine_name, table_comment
                    )
                    VALUES (
                        :ns, :sid, :tname, NULL,
                        NULL, NULL, CAST(:pk AS jsonb), 'oracle', NULL
                    )
                    ON CONFLICT (namespace, table_name, schema_name) DO UPDATE
                    SET snapshot_id = EXCLUDED.snapshot_id,
                        primary_key = EXCLUDED.primary_key,
                        updated_at  = NOW()
                    RETURNING id
                    """
                ),
                {
                    "ns": ns,
                    "sid": snapshot_id,
                    "tname": table_name,
                    "pk": json.dumps(pk_cols),
                },
            ).scalar_one()

            conn.execute(
                text(
                    "DELETE FROM mem_columns WHERE namespace = :ns AND table_id = :tid"
                ),
                {"ns": ns, "tid": table_row_id},
            )

            for col in columns:
                conn.execute(
                    text(
                        """
                        INSERT INTO mem_columns(
                            namespace, table_id, column_name, data_type,
                            is_nullable, default_value, max_length,
                            numeric_precision, numeric_scale, is_primary
                        )
                        VALUES (
                            :ns, :tid, :cname, :dtype,
                            :nullable, :dflt, :len,
                            :prec, :scale, :is_pk
                        )
                        """
                    ),
                    {
                        "ns": ns,
                        "tid": table_row_id,
                        "cname": col.get("name"),
                        "dtype": str(col.get("type")),
                        "nullable": bool(col.get("nullable", True)),
                        "dflt": col.get("default"),
                        "len": col.get("length"),
                        "prec": col.get("precision"),
                        "scale": col.get("scale"),
                        "is_pk": col.get("name") in pk_cols,
                    },
                )

            processed.append(table_name)

    return jsonify(
        {
            "ok": not errors,
            "namespace": ns,
            "tables": processed,
            "errors": errors,
        }
    )


@dw_bp.route("/dw/metrics", methods=["GET"])
def metrics() -> Any:
    ns = _namespace({})
    settings = Settings(namespace=ns)
    mem = get_mem_engine(settings)

    with mem.connect() as conn:
        result = conn.execute(
            text(
                """
                SELECT metric_key, metric_name, description, calculation_sql,
                       category, owner, is_active
                  FROM mem_metrics
                 WHERE namespace = :ns
                 ORDER BY metric_key
                """
            ),
            {"ns": ns},
        ).mappings().all()

    metrics_rows = [dict(row) for row in result]
    return jsonify({"ok": True, "namespace": ns, "metrics": metrics_rows})


@dw_bp.route("/dw/answer", methods=["POST"])
def answer() -> Any:
    payload = _payload()
    question = payload.get("question") or ""
    auth_email = payload.get("auth_email")
    prefixes = payload.get("prefixes") or []
    ns = _namespace(payload)

    pipeline = current_app.config.get("pipeline")
    if not pipeline:
        return jsonify({"ok": False, "error": "Pipeline not available"}), 500

    try:
        result = pipeline.answer(
            question=question,
            auth_email=auth_email,
            prefixes=prefixes,
            datasource="docuware",
            namespace=ns,
        )
    except NotImplementedError as exc:  # pragma: no cover - legacy stub
        return jsonify({"ok": False, "error": str(exc)}), 501

    return jsonify(result)


def create_dw_blueprint(settings: Settings | None = None) -> Blueprint:
    """Compatibility factory for legacy imports."""
    return dw_bp
