import logging
import os
from typing import Any, Dict, List

import yaml

from apps.dw.intent import parse_intent
from apps.dw.planner import build_sql

GOLDEN_PATH = os.getenv("DW_GOLDEN_PATH", "apps/dw/tests/golden_dw_contracts.yaml")


def _load_yaml(path: str) -> Dict[str, Any] | None:
    if not os.path.exists(path):
        logging.warning(f"[golden] YAML not found at: {path}")
        return None
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if not isinstance(data, dict):
        logging.error("[golden] YAML root must be a mapping (dict).")
        return None
    return data


def _ci(s: str) -> str:
    return (s or "").upper()


def _check_sql(sql: str, expect: Dict[str, Any]) -> tuple[bool, List[str]]:
    failures: List[str] = []
    src = _ci(sql)
    must = [m for m in (expect or {}).get("must_contain", [])]
    must_not = [m for m in (expect or {}).get("must_not_contain", [])]
    for m in must:
        if _ci(m) not in src:
            failures.append(f"must_contain not found: {m}")
    for m in must_not:
        if _ci(m) in src:
            failures.append(f"must_not_contain present: {m}")
    return (len(failures) == 0, failures)


def run_golden_tests(namespace: str = "dw::common") -> Dict[str, Any]:
    data = _load_yaml(GOLDEN_PATH) or {}
    ns_in_file = data.get("namespace")
    if ns_in_file and ns_in_file != namespace:
        logging.info(
            f"[golden] YAML namespace={ns_in_file} differs from requested {namespace} (continuing)."
        )
    cases = data.get("cases") or []
    if not isinstance(cases, list):
        logging.error("[golden] YAML missing 'cases' as a list.")
        return {"ok": True, "total": 0, "passed": 0, "results": []}
    logging.info(f"[golden] loaded {len(cases)} cases from {GOLDEN_PATH}")

    results: List[Dict[str, Any]] = []
    passed = 0
    for idx, case in enumerate(cases, 1):
        q = case.get("question", "")
        cid = case.get("id") or f"case_{idx}"
        try:
            intent = parse_intent(q, namespace=namespace)
            sql, _meta = build_sql(intent, namespace=namespace, dry_run=True)
            ok, fails = _check_sql(sql or "", case.get("expect", {}))
            results.append(
                {
                    "id": cid,
                    "question": q,
                    "sql": sql,
                    "passed": ok,
                    "failures": fails,
                }
            )
            if ok:
                passed += 1
        except Exception as ex:
            results.append(
                {
                    "id": cid,
                    "question": q,
                    "sql": None,
                    "passed": False,
                    "failures": [f"exception: {type(ex).__name__}: {ex}"],
                }
            )
    return {"ok": True, "total": len(cases), "passed": passed, "results": results}
