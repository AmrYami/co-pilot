#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Export DB-backed context to docs/state/*.json
Usage:
  MEMORY_DB_URL=postgresql+psycopg2://... python scripts/export_context.py --out docs/state
"""
from __future__ import annotations
import os, json, argparse
from typing import Optional, Iterable, Set
from sqlalchemy import create_engine, text, inspect
try:
    # Optional: load .env if present
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass

def _pick(cols: Iterable[str], *candidates: str) -> Optional[str]:
    s = {c.lower() for c in cols}
    for c in candidates:
        if c and c.lower() in s:
            return c
    return None

def table_exists(engine, name: str, schema: str | None = None) -> bool:
    """Compatibility wrapper that works with SQLAlchemy 2.x."""

    try:
        insp = inspect(engine)
        candidates = [name]
        if schema:
            return insp.has_table(name, schema=schema)
        return any(
            insp.has_table(candidate, schema=schema)
            for candidate in (name, name.lower(), name.upper())
        )
    except Exception:
        return False

def table_columns(engine, name: str, schema: str | None = None) -> Set[str]:
    try:
        with engine.connect() as conn:
            clauses = ["lower(table_name)=lower(:t)"]
            params = {"t": name}
            if schema:
                clauses.append("lower(table_schema)=lower(:s)")
                params["s"] = schema
            sql = f"SELECT column_name FROM information_schema.columns WHERE {' AND '.join(clauses)}"
            rows = conn.execute(text(sql), params or {}).mappings().all()
            return {row.get("column_name") for row in rows if row.get("column_name")}
    except Exception:
        return set()

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

    # Read from env (works with .env). Keep default for local dev.
    url = os.environ.get("MEMORY_DB_URL", "postgresql+psycopg2://postgres:123456789@localhost/copilot_mem_dev")
    if not url:
        for f in ("settings_export.json","examples_export.json","rules_export.json","patches_export.json","runs_metrics_24h.json"):
            with open(os.path.join(args.out, f), "w") as fp:
                json.dump({"warning":"MEMORY_DB_URL not set"}, fp, indent=2)
        print("MEMORY_DB_URL not set; wrote placeholders to", args.out)
        return 0

    eng = create_engine(url, pool_pre_ping=True)  # 2.0-safe

    # settings
    settings = dump(eng, """
        SELECT key, value, value_type, scope
        FROM mem_settings
        WHERE namespace='dw::common'
        ORDER BY key
    """)
    with open(os.path.join(args.out, "settings_export.json"), "w") as fp:
      json.dump(settings, fp, indent=2, default=str)

    if table_exists(eng, "dw_examples"):
        examples = dump(
            eng,
            """
        SELECT q_norm AS question,
               sql AS sql,
               success_count,
               COALESCE(updated_at, created_at) AS updated_at
          FROM dw_examples
         ORDER BY COALESCE(updated_at, created_at) DESC
         LIMIT 1000
        """,
        )
    else:
        examples = {"warning":"dw_examples not found"}
    with open(os.path.join(args.out, "examples_export.json"), "w") as fp:
      json.dump(examples, fp, indent=2, default=str)

    # rules (order by updated_at if exists, else created_at, else id)
    if table_exists(eng, "dw_rules"):
        r_cols = table_columns(eng, "dw_rules")
        order_col = _pick(r_cols, "updated_at", "created_at", "id") or "id"
        rules = dump(eng, f"SELECT * FROM dw_rules ORDER BY {order_col} DESC")
    else:
        rules = {"warning":"dw_rules not found"}
    with open(os.path.join(args.out, "rules_export.json"), "w") as fp:
      json.dump(rules, fp, indent=2, default=str)

    # patches (prefer created_at if exists)
    if table_exists(eng, "dw_patches"):
        p_cols = table_columns(eng, "dw_patches")
        order_col = _pick(p_cols, "created_at", "updated_at", "id") or "id"
        patches = dump(eng, f"SELECT * FROM dw_patches ORDER BY {order_col} DESC")
    else:
        patches = {"warning":"dw_patches not found"}
    with open(os.path.join(args.out, "patches_export.json"), "w") as fp:
      json.dump(patches, fp, indent=2, default=str)

    # runs metrics 24h
    if table_exists(eng, "dw_runs"):
        r_cols = table_columns(eng, "dw_runs")
        ok_col = _pick(r_cols, "ok", "success", "is_ok") or "ok"
        ts_col = _pick(r_cols, "created_at", "updated_at", "ts", "timestamp") or "created_at"
        metrics = dump(eng, f"""
            SELECT COUNT(*) AS total,
                   SUM(CASE WHEN {ok_col} THEN 1 ELSE 0 END) AS ok
            FROM dw_runs
            WHERE {ts_col} >= NOW() - INTERVAL '24 hour'
        """)
    else:
        metrics = {"warning":"dw_runs not found"}
    with open(os.path.join(args.out, "runs_metrics_24h.json"), "w") as fp:
      json.dump(metrics, fp, indent=2, default=str)

    print("Exported to:", args.out)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
