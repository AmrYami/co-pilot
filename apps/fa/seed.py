from __future__ import annotations
import os, glob, json
from typing import Dict, Any, List
import yaml
from sqlalchemy import text
from core.settings import Settings


def _upsert_join_graph(conn, namespace: str, items: List[Dict[str, Any]]) -> int:
    sql = text(
        """
        INSERT INTO mem_join_graph(namespace, from_table, from_column, to_table, to_column,
                                   join_type, cardinality, is_preferred, confidence, discovered_by)
        VALUES (:ns, :ft, :fc, :tt, :tc, :jt, :card, :pref, 1.0, 'manual')
        ON CONFLICT (namespace, from_table, from_column, to_table, to_column)
        DO UPDATE SET
            join_type    = EXCLUDED.join_type,
            cardinality  = EXCLUDED.cardinality,
            is_preferred = EXCLUDED.is_preferred,
            updated_at   = NOW()
    """
    )
    n = 0
    for it in items:
        params = {
            "ns": namespace,
            "ft": it["from_table"], "fc": it["from_column"],
            "tt": it["to_table"],   "tc": it["to_column"],
            "jt": it.get("join_type", "INNER"),
            "card": it.get("cardinality"),
            "pref": bool(it.get("is_preferred", False)),
        }
        conn.execute(sql, params)
        n += 1
    return n


def _upsert_metric(conn, namespace: str, m: Dict[str, Any]) -> None:
    sql = text(
        """
        INSERT INTO mem_metrics(namespace, metric_key, metric_name, description, calculation_sql,
                                required_tables, required_columns, parameters, category, owner, version,
                                is_active, verified_at, created_at, updated_at)
        VALUES (:ns, :key, :name, :desc, :calc,
                :req_tables, :req_cols, :params, :cat, :owner, :ver,
                true, NOW(), NOW(), NOW())
        ON CONFLICT (namespace, metric_key, version)
        DO UPDATE SET
            metric_name      = EXCLUDED.metric_name,
            description      = EXCLUDED.description,
            calculation_sql  = EXCLUDED.calculation_sql,
            required_tables  = EXCLUDED.required_tables,
            required_columns = EXCLUDED.required_columns,
            parameters       = EXCLUDED.parameters,
            category         = EXCLUDED.category,
            owner            = EXCLUDED.owner,
            is_active        = true,
            updated_at       = NOW()
    """
    )
    conn.execute(
        sql,
        {
            "ns": namespace,
            "key": m["metric_key"],
            "name": m.get("metric_name"),
            "desc": m.get("description"),
            "calc": m["calculation_sql"],
            "req_tables": json.dumps(m.get("required_tables") or []),
            "req_cols": json.dumps(m.get("required_columns") or []),
            "params": json.dumps(m.get("parameters") or {}),
            "cat": m.get("category"),
            "owner": m.get("owner"),
            "ver": int(m.get("version") or 1),
        },
    )


def seed_if_missing(mem_engine, namespace: str, settings: Settings, force: bool = False) -> Dict[str, Any]:
    """
    Load apps/fa/join_graph.yaml and apps/fa/metrics/*.yaml
    into mem_join_graph and mem_metrics. If 'force' is False, we still upsert idempotently.
    """
    join_path = settings.get("FA_JOIN_GRAPH_PATH", namespace=namespace) or "apps/fa/join_graph.yaml"
    metrics_dir = settings.get("FA_METRICS_PATH", namespace=namespace) or "apps/fa/metrics"

    # read files
    with open(join_path, "r", encoding="utf-8") as f:
        join_items = yaml.safe_load(f) or []

    metric_files = sorted(glob.glob(os.path.join(metrics_dir, "*.yaml")))
    metrics: List[Dict[str, Any]] = []
    for fp in metric_files:
        with open(fp, "r", encoding="utf-8") as f:
            metrics.append(yaml.safe_load(f) or {})

    inserted = {"join_graph": 0, "metrics": 0}
    with mem_engine.begin() as conn:
        inserted["join_graph"] = _upsert_join_graph(conn, namespace, join_items)
        for m in metrics:
            if m and "metric_key" in m and "calculation_sql" in m:
                _upsert_metric(conn, namespace, m)
                inserted["metrics"] += 1
    return inserted
