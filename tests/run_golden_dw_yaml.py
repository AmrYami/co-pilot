from __future__ import annotations

import argparse
import datetime as dt
import json
import pathlib
import re
import sys
import time
from typing import Any, Dict, List

try:
    import yaml  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - optional dependency
    yaml = None  # type: ignore

try:
    from flask import Flask
except ModuleNotFoundError:  # pragma: no cover - helpful message when Flask missing
    Flask = None  # type: ignore
    _FLASK_IMPORT_ERROR = True
else:  # pragma: no cover - executed when Flask available
    _FLASK_IMPORT_ERROR = False

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:  # pragma: no cover - path setup
    sys.path.insert(0, str(ROOT))


def _normalize_sql(sql: str) -> str:
    return re.sub(r"\s+", " ", (sql or "")).strip()


def _load_cases(path: pathlib.Path) -> List[Dict[str, Any]]:
    text = path.read_text(encoding="utf-8")
    if yaml is not None:
        data = yaml.safe_load(text)
        if isinstance(data, dict) and "cases" in data:
            cases = data["cases"]
        else:
            cases = data
    else:  # pragma: no cover - simple manual parser for minimal envs
        cases = []
        current: Dict[str, Any] | None = None
        for raw in text.splitlines():
            stripped = raw.strip()
            if not stripped or stripped.startswith("#"):
                continue
            if stripped.startswith("- q:"):
                if current:
                    cases.append(current)
                current = {
                    "q": stripped.split(":", 1)[1].strip().strip("'\"")
                }
                continue
            if current is None:
                continue
            if stripped.startswith("expect_sql_contains:"):
                current["expect_sql_contains"] = []
                continue
            if stripped.startswith("binds_like:"):
                current.setdefault("binds_like", {})
                continue
            if stripped.startswith("expect_rows_like:"):
                current["expect_rows_like"] = []
                continue
            if stripped.startswith("-") and "expect_sql_contains" in current:
                frag = stripped[1:].strip().strip("'\"")
                current.setdefault("expect_sql_contains", []).append(frag)
                continue
        if current:
            cases.append(current)
    return list(cases or [])


def _resolve_expected(value: Any, today: dt.date) -> Any:
    if isinstance(value, str) and value.startswith("TODAY"):
        offset = 0
        if len(value) > 5:
            try:
                offset = int(value[5:])
            except Exception:
                offset = 0
        return (today + dt.timedelta(days=offset)).isoformat()
    return value


def _compare_binds(actual: Dict[str, Any], expected: Dict[str, Any]) -> List[str]:
    errs: List[str] = []
    today = dt.date.today()
    for key, exp in expected.items():
        want = _resolve_expected(exp, today)
        got = actual.get(key)
        if want is None:
            if got is not None:
                errs.append(f"bind {key} expected None, got {got}")
            continue
        if isinstance(want, str) and isinstance(got, str):
            if want != got:
                errs.append(f"bind {key} expected {want}, got {got}")
            continue
        if want != got:
            errs.append(f"bind {key} expected {want}, got {got}")
    return errs


def _run_case(client, case: Dict[str, Any]) -> Dict[str, Any]:
    question = case.get("q") or case.get("question")
    payload = {
        "prefixes": [],
        "question": question,
        "auth_email": "dev@example.com",
    }
    t0 = time.time()
    rv = client.post("/dw/answer", json=payload)
    ms = int((time.time() - t0) * 1000)
    data = rv.get_json(silent=True) or {}
    sql = _normalize_sql(data.get("sql", ""))
    rows = data.get("rows") or []
    meta = data.get("meta") or {}
    binds = meta.get("binds") or {}

    errors: List[str] = []

    for frag in case.get("expect_sql_contains", []) or []:
        if frag not in sql:
            errors.append(f"missing fragment: {frag}")

    binds_like = case.get("binds_like")
    if isinstance(binds_like, dict):
        errors.extend(_compare_binds(binds, binds_like))

    expected_rows = case.get("expect_rows_like")
    if expected_rows is not None:
        if not isinstance(expected_rows, list):
            errors.append("expect_rows_like must be list")
        else:
            if rows[: len(expected_rows)] != expected_rows:
                errors.append(f"rows mismatch: expected {expected_rows}, got {rows[:len(expected_rows)]}")

    return {
        "id": case.get("id") or question,
        "question": question,
        "ok": not errors and bool(data.get("ok")),
        "errors": errors,
        "sql": sql,
        "rowcount": len(rows),
        "duration_ms": ms,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--cases", default="tests/golden_dw.yml")
    args = ap.parse_args()

    cases_path = pathlib.Path(args.cases)
    cases = _load_cases(cases_path)

    if _FLASK_IMPORT_ERROR:
        print(json.dumps({"error": "Flask import failed. Install Flask to run DW golden tests."}))
        return 1

    import main as app_module  # Local import to avoid loading when Flask missing

    app: Flask = app_module.create_app()
    client = app.test_client()

    results = [_run_case(client, case) for case in cases]
    print(json.dumps({"results": results}, indent=2))
    failed = [r for r in results if not r["ok"]]
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
