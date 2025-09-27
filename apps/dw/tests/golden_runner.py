from apps.dw.planner import build_sql
import os
import yaml

GOLDEN_PATH = os.environ.get("DW_GOLDEN_PATH", "apps/dw/tests/golden_dw_contracts.yaml")


def _load_yaml(path: str):
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
        # Expecting either {"cases":[...]} or a raw list
        if isinstance(data, list):
            return {"cases": data}
        if "cases" not in data:
            data["cases"] = []
        return data


def run_golden_tests(*, namespace: str) -> dict:
    data = _load_yaml(GOLDEN_PATH)
    cases = data.get("cases", [])
    results = []
    passed = 0
    for i, case in enumerate(cases, 1):
        q = case.get("q") or case.get("question")
        expect = case.get("expect", {})
        intent = case.get("intent", {})
        # Minimal FTS columns (optional)
        fts_cols = case.get("fts_columns")
        sql, binds = build_sql(q, intent, table="Contract", fts_columns=fts_cols)
        ok = True
        notes = []
        contains = expect.get("contains", [])
        for frag in contains:
            if frag not in sql:
                ok = False
                notes.append(f"missing '{frag}'")
        not_contains = expect.get("not_contains", [])
        for frag in not_contains:
            if frag in sql:
                ok = False
                notes.append(f"should not contain '{frag}'")
        if ok:
            passed += 1
        results.append({"i": i, "q": q, "ok": ok, "sql": sql, "binds": binds, "notes": notes})
    return {"ok": True, "total": len(cases), "passed": passed, "results": results}
