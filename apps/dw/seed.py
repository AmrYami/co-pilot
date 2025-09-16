"""Seed DocuWare knowledge (metrics and joins) into the memory database."""

from __future__ import annotations

import json
from pathlib import Path

import yaml
from sqlalchemy import text

BASE = Path(__file__).parent
METRICS_DIR = BASE / "metrics"
JOIN_GRAPH_FILE = BASE / "join_graph.yaml"


def _upsert_metric(conn, namespace: str, metric: dict) -> None:
    conn.execute(
        text(
            """
        INSERT INTO mem_metrics(namespace, metric_key, metric_name, description,
                                calculation_sql, required_tables, required_columns,
                                parameters, category, owner, version, is_active, verified_at)
        VALUES (:ns, :metric_key, :metric_name, :description, :calculation_sql,
                :required_tables, :required_columns, :parameters, :category, :owner,
                :version, true, NOW())
        ON CONFLICT (namespace, metric_key, version)
        DO UPDATE SET
            calculation_sql = EXCLUDED.calculation_sql,
            description     = EXCLUDED.description,
            updated_at      = NOW(),
            is_active       = true
        """
        ),
        {
            "ns": namespace,
            "metric_key": metric["metric_key"],
            "metric_name": metric.get("metric_name") or metric["metric_key"],
            "description": metric.get("description"),
            "calculation_sql": metric["calculation_sql"],
            "required_tables": json.dumps(metric.get("required_tables") or []),
            "required_columns": json.dumps(metric.get("required_columns") or []),
            "parameters": json.dumps(metric.get("parameters") or {}),
            "category": metric.get("category"),
            "owner": metric.get("owner"),
            "version": int(metric.get("version", 1)),
        },
    )


def _upsert_join(conn, namespace: str, join: dict) -> None:
    conn.execute(
        text(
            """
        INSERT INTO mem_join_graph(namespace, from_table, from_column, to_table, to_column,
                                   join_type, cardinality, is_preferred, confidence, discovered_by)
        VALUES(:ns, :from_table, :from_column, :to_table, :to_column, :join_type,
               :cardinality, :is_preferred, :confidence, 'manual')
        ON CONFLICT (namespace, from_table, from_column, to_table, to_column)
        DO UPDATE SET
           updated_at = NOW(),
           usage_count = COALESCE(mem_join_graph.usage_count, 0)
        """
        ),
        {
            "ns": namespace,
            "from_table": join["from_table"],
            "from_column": join["from_column"],
            "to_table": join["to_table"],
            "to_column": join["to_column"],
            "join_type": join.get("join_type", "INNER"),
            "cardinality": join.get("cardinality"),
            "is_preferred": bool(join.get("is_preferred", False)),
            "confidence": float(join.get("confidence", 1.0)),
        },
    )


def seed_dw_knowledge(mem_engine, namespace: str, force: bool = False) -> dict:
    """Load DocuWare metrics and joins into the memory store."""

    inserted_metrics = 0
    inserted_joins = 0

    with mem_engine.begin() as conn:
        for metric_file in METRICS_DIR.glob("*.y*ml"):
            data = yaml.safe_load(metric_file.read_text(encoding="utf-8")) or {}
            for metric in data.get("metrics", []):
                _upsert_metric(conn, namespace, metric)
                inserted_metrics += 1

        if JOIN_GRAPH_FILE.exists():
            graph_data = yaml.safe_load(JOIN_GRAPH_FILE.read_text(encoding="utf-8")) or {}
            for join in graph_data.get("joins", []):
                _upsert_join(conn, namespace, join)
                inserted_joins += 1

    return {"metrics": inserted_metrics, "join_graph": inserted_joins}
