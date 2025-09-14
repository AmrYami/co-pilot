from __future__ import annotations
from typing import Any, Dict
from sqlalchemy import text
import yaml, os, glob, json

def seed_fa_knowledge(mem_engine, namespace: str, metrics_dir: str, join_graph_path: str) -> Dict[str, Any]:
    inserted = {"metrics": 0, "joins": 0}
    with mem_engine.begin() as c:
        # Insert join graph
        if os.path.exists(join_graph_path):
            data = yaml.safe_load(open(join_graph_path, "r", encoding="utf-8")) or {}
            for j in data.get("joins", []):
                c.execute(text("""
                    INSERT INTO mem_join_graph(namespace, from_table, from_column, to_table, to_column,
                                               join_type, cardinality, is_preferred, confidence)
                    VALUES (:ns, :ft, :fc, :tt, :tc, :jt, :card, COALESCE(:pref,false), COALESCE(:conf,1.0))
                    ON CONFLICT (namespace, from_table, from_column, to_table, to_column) DO NOTHING
                """), {
                    "ns": namespace,
                    "ft": j.get("from_table"), "fc": j.get("from_column"),
                    "tt": j.get("to_table"),   "tc": j.get("to_column"),
                    "jt": j.get("join_type", "INNER"),
                    "card": j.get("cardinality"),
                    "pref": j.get("is_preferred"),
                    "conf": j.get("confidence", 1.0)
                })
                inserted["joins"] += 1

        # Insert metrics
        if os.path.isdir(metrics_dir):
            for mf in sorted(glob.glob(os.path.join(metrics_dir, "*.yaml"))):
                data = yaml.safe_load(open(mf, "r", encoding="utf-8")) or {}
                for key, meta in (data.get("metrics") or {}).items():
                    c.execute(text("""
                        INSERT INTO mem_metrics(namespace, metric_key, metric_name, description,
                                                calculation_sql, required_tables, required_columns,
                                                parameters, category, owner, version, is_active)
                        VALUES (:ns, :key, :name, :desc, :sql, :rt, :rc, :params, :cat, :owner, :ver, true)
                        ON CONFLICT (namespace, metric_key, version) DO NOTHING
                    """), {
                        "ns": namespace,
                        "key": key,
                        "name": meta.get("name") or key,
                        "desc": meta.get("description"),
                        "sql":  meta.get("calculation_sql"),
                        "rt":   json.dumps(meta.get("required_tables") or []),
                        "rc":   json.dumps(meta.get("required_columns") or []),
                        "params": json.dumps(meta.get("parameters") or {}),
                        "cat":  meta.get("category"),
                        "owner": meta.get("owner"),
                        "ver":  int(meta.get("version", 1)),
                    })
                    inserted["metrics"] += 1

    return inserted
