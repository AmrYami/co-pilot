#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Export DB-backed context to docs/state/*.json
Usage:
  MEMORY_DB_URL=postgresql+psycopg2://... python scripts/export_context.py --out docs/state
"""
from __future__ import annotations
import os, json, argparse
from sqlalchemy import create_engine, text, inspect
try:
    # Optional: load .env if available
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass

def table_exists(engine, name: str, schema: str | None = None) -> bool:
    """
    SQLAlchemy 2.0-safe table existence check.
    Try name as-is, lower, and upper to be robust to case/quoting.
    """
    try:
        insp = inspect(engine)
        for cand in (name, name.lower(), name.upper()):
            if insp.has_table(cand, schema=schema):
                return True
        return False
    except Exception:
        return False

def dump(engine, sql: str, params=None):
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(sql), params or {}).mappings().all()
            return [dict(r) for r in rows]
    except Exception as e:
        return {"error": str(e), "sql": sql}

def main():
    import sys
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="docs/state", help="Output directory")
    args = ap.parse_args()
    os.makedirs(args.out, exist_ok=True)

    url = os.getenv("MEMORY_DB_URL", "postgresql+psycopg2://postgres:123456789@localhost/copilot_mem_dev")
    if not url:
        for f in ("settings_export.json","examples_export.json","rules_export.json","patches_export.json","runs_metrics_24h.json"):
            with open(os.path.join(args.out, f), "w") as fp:
                json.dump({"warning":"MEMORY_DB_URL not set"}, fp, indent=2)
        print("MEMORY_DB_URL not set; wrote placeholders to", args.out)
        return 0

    eng = create_engine(url, pool_pre_ping=True)

    # settings
    settings = dump(eng, """
      SELECT key, value, value_type, scope
      FROM mem_settings
      WHERE namespace='dw::common'
      ORDER BY key
    """)
    with open(os.path.join(args.out, "settings_export.json"), "w") as fp:
      json.dump(settings, fp, indent=2, default=str)

    # examples
    examples = (dump(eng, "SELECT q_norm AS question, sql AS sql, success_count, created_at FROM dw_examples ORDER BY created_at DESC LIMIT 1000")
                if table_exists(eng, "dw_examples") else {"warning":"dw_examples not found"})
    with open(os.path.join(args.out, "examples_export.json"), "w") as fp:
      json.dump(examples, fp, indent=2, default=str)

    # rules
    rules = (dump(eng, "SELECT * FROM dw_rules ORDER BY updated_at DESC")
             if table_exists(eng, "dw_rules") else {"warning":"dw_rules not found"})
    with open(os.path.join(args.out, "rules_export.json"), "w") as fp:
      json.dump(rules, fp, indent=2, default=str)

    # patches
    patches = (dump(eng, "SELECT * FROM dw_patches ORDER BY created_at DESC")
               if table_exists(eng, "dw_patches") else {"warning":"dw_patches not found"})
    with open(os.path.join(args.out, "patches_export.json"), "w") as fp:
      json.dump(patches, fp, indent=2, default=str)

    # runs metrics 24h
    if table_exists(eng, "dw_runs"):
        metrics = dump(eng, """
          SELECT COUNT(*) AS total,
                 SUM(CASE WHEN ok THEN 1 ELSE 0 END) AS ok
          FROM dw_runs
          WHERE created_at >= NOW() - INTERVAL '24 hour'
        """)
    else:
        metrics = {"warning":"dw_runs not found"}
    with open(os.path.join(args.out, "runs_metrics_24h.json"), "w") as fp:
      json.dump(metrics, fp, indent=2, default=str)

    print("Exported to:", args.out)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
