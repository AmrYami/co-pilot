"""Golden tests runner for DW app.
Executes the existing /dw/answer endpoint inside a Flask test request context
and validates that the produced SQL contains required substrings.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List

from flask import current_app
import yaml

GOLDEN_PATH = os.environ.get(
    "DW_GOLDEN_PATH", "apps/dw/tests/golden_dw_contracts.yaml"
)


def _load_yaml(path: str) -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {"tests": []}
    with p.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
        if not isinstance(data, dict):
            data = {"tests": data}
        if "tests" not in data or not isinstance(data["tests"], list):
            data["tests"] = []
        return data


def _call_dw_answer(question: str, namespace: str = "dw::common") -> Dict[str, Any]:
    """Invoke the /dw/answer view directly in a request context and return JSON."""
    # Lazy import to avoid circular imports on app startup
    from apps.dw.app import answer as dw_answer

    payload = {
        "prefixes": [],
        "question": question,
        "auth_email": "golden@tests",
    }
    if namespace:
        payload["namespace"] = namespace
    with current_app.test_request_context(
        "/dw/answer",
        method="POST",
        json=payload,
        headers={"Content-Type": "application/json"},
    ):
        resp = dw_answer()
        # resp can be a (json, code) or flask.Response
        try:
            data = resp.get_json()  # type: ignore[attr-defined]
        except Exception:
            data = resp  # already dict
        if not isinstance(data, dict):
            return {"ok": False, "error": "non-dict response"}
        return data


def run_golden_tests(namespace: str = "dw::common") -> Dict[str, Any]:
    data = _load_yaml(GOLDEN_PATH)
    tests: List[Dict[str, Any]] = data.get("tests", [])
    results: List[Dict[str, Any]] = []
    passed = 0
    for t in tests:
        name = t.get("name") or t.get("question", "")[:60]
        q = t.get("question", "")
        skip = bool(t.get("skip"))
        expect_contains: List[str] = t.get("expect_contains") or []
        item_res: Dict[str, Any] = {"name": name, "question": q, "skip": skip}
        if skip:
            item_res["status"] = "skipped"
            results.append(item_res)
            continue
        out = _call_dw_answer(q, namespace=namespace)
        item_res["response"] = out
        sql = (out or {}).get("sql", "") if isinstance(out, dict) else ""
        missing = [frag for frag in expect_contains if frag not in sql]
        if missing:
            item_res["status"] = "failed"
            item_res["missing"] = missing
        else:
            item_res["status"] = "passed"
            passed += 1
        results.append(item_res)
    return {"ok": True, "total": len(tests), "passed": passed, "results": results}
