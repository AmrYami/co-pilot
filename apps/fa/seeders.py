from __future__ import annotations
from typing import Any, Dict, List, Optional
import os
import json

try:
    import yaml  # type: ignore
except Exception:
    yaml = None

from sqlalchemy import text, Engine


def _load_yaml_file(path: str) -> Optional[Any]:
    if not path or not os.path.exists(path):
        return None
    if yaml is None:
        # minimal fallback: try JSON if user accidentally provided json
        with open(path, "r", encoding="utf-8") as f:
            raw = f.read()
        try:
            return json.loads(raw)
        except Exception:
            raise RuntimeError("pyyaml is not installed and file isn't JSON; install pyyaml to import YAML.")
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def seed_join_graph(mem: Engine, namespace: str, path: str) -> Dict[str, Any]:
    """
    Import app join graph into mem_join_graph.
    Expected YAML shape (examples):
    - from: debtor_trans
      from_column: debtor_no
      to: debtors_master
      to_column: debtor_no
      join_type: INNER       # optional, default INNER
      cardinality: N:1       # optional
      is_preferred: true     # optional
    """
    payload = _load_yaml_file(path) or []
    if not isinstance(payload, list):
        raise ValueError("join_graph.yaml must be a list of edges.")

    upsert = text(
        """
        INSERT INTO mem_join_graph(
            namespace, from_table, from_column, to_table, to_column,
            join_type, cardinality, is_preferred, confidence, discovered_by, usage_count, updated_at, created_at
        )
        VALUES (
            :ns, :ft, :fc, :tt, :tc,
            COALESCE(:jt, 'INNER'), :card, COALESCE(:pref, false), 1.00, 'import', 0, NOW(), NOW()
        )
        ON CONFLICT (namespace, from_table, from_column, to_table, to_column)
        DO UPDATE SET
            join_type    = EXCLUDED.join_type,
            cardinality  = EXCLUDED.cardinality,
            is_preferred = EXCLUDED.is_preferred,
            updated_at   = NOW();
        """
    )

    inserted = 0
    with mem.begin() as conn:
        for edge in payload:
            params = {
                "ns": namespace,
                "ft": edge.get("from") or edge.get("from_table"),
                "fc": edge.get("from_column"),
                "tt": edge.get("to") or edge.get("to_table"),
                "tc": edge.get("to_column"),
                "jt": edge.get("join_type"),
                "card": edge.get("cardinality"),
                "pref": bool(edge.get("is_preferred", False)),
            }
            if not all([params["ft"], params["fc"], params["tt"], params["tc"]]):
                continue
            conn.execute(upsert, params)
            inserted += 1
    return {"ok": True, "count": inserted}


def seed_metrics(mem: Engine, namespace: str, metrics_dir: str) -> Dict[str, Any]:
    """
    Import metrics from a folder of YAML or JSON files.
    Each file should contain at least:
      metric_key: net_sales
      calculation_sql: "SUM(...)"
      metric_name: "Net Sales"           # optional
      description: "..."                 # optional
      required_tables: ["debtor_trans", "debtor_trans_details", "debtors_master"]
      required_columns: ["dt.tran_date", "dm.name", ...]   # optional
      category: "sales"                  # optional
    """
    if not metrics_dir or not os.path.isdir(metrics_dir):
        return {"ok": True, "count": 0}

    files = [f for f in os.listdir(metrics_dir) if f.endswith((".yml", ".yaml", ".json"))]
    upsert = text(
        """
        INSERT INTO mem_metrics(
            namespace, metric_key, metric_name, description,
            calculation_sql, required_tables, required_columns, category,
            version, is_active, created_at, updated_at
        )
        VALUES (
            :ns, :key, :name, :desc,
            :sql, :req_tables, :req_cols, :cat,
            COALESCE(:ver, 1), true, NOW(), NOW()
        )
        ON CONFLICT (namespace, metric_key, version)
        DO UPDATE SET
            metric_name      = EXCLUDED.metric_name,
            description      = EXCLUDED.description,
            calculation_sql  = EXCLUDED.calculation_sql,
            required_tables  = EXCLUDED.required_tables,
            required_columns = EXCLUDED.required_columns,
            category         = EXCLUDED.category,
            updated_at       = NOW();
        """
    )

    count = 0
    with mem.begin() as conn:
        for fname in files:
            full = os.path.join(metrics_dir, fname)
            data = _load_yaml_file(full)
            if not isinstance(data, dict):
                continue
            key = data.get("metric_key")
            sql = data.get("calculation_sql") or data.get("expr")
            if not key or not sql:
                continue
            params = {
                "ns": namespace,
                "key": key,
                "name": data.get("metric_name") or key.replace("_", " ").title(),
                "desc": data.get("description"),
                "sql": sql,
                "req_tables": json.dumps(data.get("required_tables") or []),
                "req_cols": json.dumps(data.get("required_columns") or []),
                "cat": data.get("category"),
                "ver": data.get("version"),
            }
            conn.execute(upsert, params)
            count += 1

    # Safety: if no metrics exist, seed a default net_sales the pipeline already used
    if count == 0:
        default_sql = (
            "SUM((CASE WHEN dt.type = 11 THEN -1 ELSE 1 END) * "
            "dtd.unit_price * (1 - COALESCE(dtd.discount_percent, 0)) * dtd.quantity)"
        )
        with mem.begin() as conn:
            conn.execute(
                upsert,
                {
                    "ns": namespace,
                    "key": "net_sales",
                    "name": "Net Sales",
                    "desc": "Invoices minus credit notes at line level (unit_price * (1-discount) * qty) signed by type.",
                    "sql": default_sql,
                    "req_tables": json.dumps(["debtor_trans", "debtor_trans_details", "debtors_master"]),
                    "req_cols": json.dumps(["dt.tran_date", "dm.name", "dtd.quantity"]),
                    "cat": "sales",
                    "ver": 1,
                },
            )
            count = 1

    return {"ok": True, "count": count}


def seed_all(mem: Engine, namespace: str, join_graph_path: str, metrics_dir: str) -> Dict[str, Any]:
    jres = seed_join_graph(mem, namespace, join_graph_path)
    mres = seed_metrics(mem, namespace, metrics_dir)
    return {"ok": True, "join_edges": jres.get("count", 0), "metrics": mres.get("count", 0)}

