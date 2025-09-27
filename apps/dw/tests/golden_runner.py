# apps/dw/tests/golden_runner.py
from __future__ import annotations
import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml
from flask import Flask

# Stable, package-relative path to the golden YAML file
GOLDEN_PATH = Path(__file__).with_name("golden_dw_contracts.yaml")
DEFAULT_NS = "dw::common"

@dataclass
class GoldenCase:
    question: str
    namespace: str = DEFAULT_NS
    prefixes: List[str] = field(default_factory=list)
    auth_email: str = "golden@local"
    full_text_search: bool = False
    # Expectations (all optional)
    expect_sql_contains: List[str] = field(default_factory=list)
    expect_group_by: List[str] = field(default_factory=list)
    expect_order_by: Optional[str] = None
    expect_date_col: Optional[str] = None  # e.g. "REQUEST_DATE", "OVERLAP", "END_DATE"
    expect_agg: Optional[str] = None       # e.g. "count", "sum"
    expect_top_n: Optional[int] = None

def _load_yaml(path: Path) -> Dict[str, Any]:
    try:
        with path.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        if not isinstance(data, dict):
            logging.warning("Golden YAML is not a dict; got: %s", type(data).__name__)
            return {"cases": []}
        if "cases" not in data or not isinstance(data["cases"], list):
            logging.warning("Golden YAML missing a 'cases' list.")
            return {"cases": []}
        return data
    except FileNotFoundError:
        logging.error("Golden YAML not found at %s", path)
        return {"cases": []}
    except Exception as e:
        logging.exception("Failed to load golden YAML: %s", e)
        return {"cases": []}

def _hydrate_case(raw: Dict[str, Any]) -> GoldenCase:
    return GoldenCase(
        question = raw.get("question", "").strip(),
        namespace = raw.get("namespace", DEFAULT_NS),
        prefixes = raw.get("prefixes", []) or [],
        auth_email = raw.get("auth_email", "golden@local"),
        full_text_search = bool(raw.get("full_text_search", False)),
        expect_sql_contains = raw.get("expect_sql_contains", []) or [],
        expect_group_by = raw.get("expect_group_by", []) or [],
        expect_order_by = raw.get("expect_order_by"),
        expect_date_col = raw.get("expect_date_col"),
        expect_agg = raw.get("expect_agg"),
        expect_top_n = raw.get("expect_top_n"),
    )

def _check_expectations(case: GoldenCase, resp: Dict[str, Any]) -> Tuple[bool, List[str]]:
    reasons: List[str] = []
    ok = True

    sql = (resp or {}).get("sql") or ""
    meta = (resp or {}).get("meta") or {}
    intent = ((resp or {}).get("debug") or {}).get("intent") or meta.get("clarifier_intent") or {}

    # 1) sql contains
    for frag in case.expect_sql_contains:
        if frag not in sql:
            ok = False
            reasons.append(f"SQL does not contain expected fragment: {frag}")

    # 2) group by
    if case.expect_group_by:
        up = sql.upper()
        if "GROUP BY" not in up:
            ok = False
            reasons.append("Expected GROUP BY, but not found.")
        else:
            for col in case.expect_group_by:
                if col.upper() not in up:
                    ok = False
                    reasons.append(f"Expected GROUP BY column missing: {col}")

    # 3) order by
    if case.expect_order_by:
        if case.expect_order_by.upper() not in sql.upper():
            ok = False
            reasons.append(f"Expected ORDER BY on: {case.expect_order_by}")

    # 4) date column intent
    if case.expect_date_col:
        # tolerate either meta.suggested_date_column or intent.date_column
        date_col = meta.get("suggested_date_column") or intent.get("date_column")
        if (date_col or "").upper() != case.expect_date_col.upper():
            ok = False
            reasons.append(f"Expected date column '{case.expect_date_col}' but got '{date_col}'")

    # 5) aggregation intent
    if case.expect_agg:
        agg = (intent.get("agg") or meta.get("agg") or "").lower()
        if agg != case.expect_agg.lower():
            ok = False
            reasons.append(f"Expected agg '{case.expect_agg}', got '{agg}'")

    # 6) top N
    if case.expect_top_n is not None:
        top_n = meta.get("binds", {}).get("top_n")
        if top_n != case.expect_top_n:
            ok = False
            reasons.append(f"Expected top_n={case.expect_top_n}, got {top_n}")

    return ok, reasons

def run_golden_tests(
    flask_app: Optional[Flask] = None,
    namespace: Optional[str] = None,
    limit: Optional[int] = None,
) -> Dict[str, Any]:
    data = _load_yaml(GOLDEN_PATH)
    raw_cases: List[Dict[str, Any]] = data.get("cases", [])

    # Namespace filter with fallback to all cases if none matched
    if namespace:
        cases = [c for c in raw_cases if c.get("namespace", DEFAULT_NS) == namespace]
        if not cases:
            logging.info("No golden cases matched namespace '%s'. Running all cases (%d).", namespace, len(raw_cases))
            cases = raw_cases
    else:
        cases = raw_cases

    # Limit if requested
    if isinstance(limit, int) and limit > 0:
        cases = cases[:limit]

    # If we do not have an app to call, just report the count
    if not flask_app:
        return {"ok": True, "total": len(cases), "passed": 0, "results": []}

    results: List[Dict[str, Any]] = []
    passed_count = 0

    with flask_app.test_client() as client:
        for idx, raw in enumerate(cases, start=1):
            case = _hydrate_case(raw)
            payload = {
                "prefixes": case.prefixes,
                "question": case.question,
                "auth_email": case.auth_email,
                "full_text_search": case.full_text_search,
            }
            try:
                rv = client.post("/dw/answer", json=payload)
                data = rv.get_json(silent=True) or {}
            except Exception as e:
                data = {"ok": False, "error": f"exception: {e}"}

            ok, reasons = _check_expectations(case, data)
            if ok:
                passed_count += 1

            results.append({
                "idx": idx,
                "namespace": case.namespace,
                "question": case.question,
                "passed": ok,
                "reasons": reasons,
                "sql": data.get("sql"),
                "meta": data.get("meta"),
            })

    return {"ok": True, "total": len(results), "passed": passed_count, "results": results}
