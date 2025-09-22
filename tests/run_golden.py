from __future__ import annotations

import argparse
import json
import re
import sys
import time
import pathlib
from typing import Any

try:
    import yaml  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - optional dependency
    yaml = None  # type: ignore
from flask import Flask

# Import your app
import main as app_module


def _normalize_sql(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def _compile_oracle(sql: str) -> tuple[bool, str]:
    # Optional: use sqlglot if present in your env
    try:
        import sqlglot

        sqlglot.parse_one(sql, read="oracle")
        return True, ""
    except Exception as e:  # pragma: no cover - best effort
        return False, str(e)


def _run_case(client, case: dict[str, Any]) -> dict[str, Any]:
    payload = {
        "prefixes": [],
        "question": case["question"],
        "auth_email": "dev@example.com",
    }
    t0 = time.time()
    rv = client.post("/dw/answer", json=payload)
    ms = int((time.time() - t0) * 1000)
    data = rv.get_json(silent=True) or {}
    sql = _normalize_sql(data.get("sql", ""))
    ok = data.get("ok", False)
    rows = data.get("rows") or []
    errs: list[str] = []

    # Must contain checks
    for frag in case.get("must_contain", []):
        if frag not in sql:
            errs.append(f"missing fragment: {frag}")

    # Compile check
    if case.get("compile_oracle"):
        good, msg = _compile_oracle(sql)
        if not good:
            errs.append(f"oracle_compile: {msg}")

    # Row checks (optional)
    if "expect_min_rows" in case:
        if len(rows) < int(case["expect_min_rows"]):
            errs.append(f"rows<{case['expect_min_rows']} (got {len(rows)})")
    if not case.get("allow_zero_rows", False) and len(rows) == 0:
        errs.append("rows==0 not allowed")

    return {
        "id": case["id"],
        "question": case["question"],
        "ok": ok and not errs,
        "errors": errs,
        "sql": sql[:300],
        "rowcount": len(rows),
        "duration_ms": ms,
    }


def _load_cases_text(text: str) -> dict[str, Any]:
    if yaml is not None:
        return yaml.safe_load(text)

    cases: list[dict[str, Any]] = []
    current: dict[str, Any] | None = None
    in_must = False
    for raw_line in text.splitlines():
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if stripped == "cases:":
            continue
        if stripped.startswith("- id:"):
            if current:
                cases.append(current)
            current = {"must_contain": []}
            in_must = False
            current["id"] = stripped.split(":", 1)[1].strip().strip("'\"")
            continue
        if current is None:
            continue
        if stripped.startswith("question:"):
            current["question"] = stripped.split(":", 1)[1].strip().strip("'\"")
            in_must = False
            continue
        if stripped.startswith("must_contain:"):
            current["must_contain"] = []
            in_must = True
            continue
        if stripped.startswith("compile_oracle:"):
            current["compile_oracle"] = stripped.split(":", 1)[1].strip().lower() == "true"
            in_must = False
            continue
        if stripped.startswith("allow_zero_rows:"):
            current["allow_zero_rows"] = stripped.split(":", 1)[1].strip().lower() == "true"
            in_must = False
            continue
        if stripped.startswith("expect_min_rows:"):
            current["expect_min_rows"] = int(stripped.split(":", 1)[1].strip())
            in_must = False
            continue
        if stripped.startswith("-") and in_must:
            frag = stripped[1:].strip().strip("'\"")
            current.setdefault("must_contain", []).append(frag)
            continue

    if current:
        cases.append(current)
    return {"cases": cases}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--cases", default="tests/golden/dw_cases.yaml")
    args = ap.parse_args()

    cases_path = pathlib.Path(args.cases)
    with cases_path.open("r", encoding="utf-8") as f:
        cfg = _load_cases_text(f.read())

    app: Flask = app_module.create_app()
    client = app.test_client()

    results = [_run_case(client, c) for c in cfg["cases"]]
    print(json.dumps({"results": results}, indent=2))
    failed = [r for r in results if not r["ok"]]
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
