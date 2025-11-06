from __future__ import annotations

from datetime import date
import subprocess
import sys
from pathlib import Path
from flask import Blueprint, current_app, jsonify, request
from typing import Any, Dict, List

# Optional imports for core NLU checks
try:  # pragma: no cover
    from core.nlu.parse import parse_intent as _parse_intent
    from core.nlu.time import resolve_window as _resolve_window
except Exception:  # pragma: no cover - keep routes importable even if missing
    _parse_intent = None  # type: ignore[assignment]
    _resolve_window = None  # type: ignore[assignment]

from .golden_runner import run_golden_tests


golden_bp = Blueprint("golden", __name__, url_prefix="/admin")
_REPO_ROOT = Path(__file__).resolve().parents[3]
_ALIAS_TEST_SUITES: Dict[str, List[str]] = {
    "eq_alias": ["apps/dw/tests/test_eq_alias_normalization.py"],
    "sql_skeleton": ["apps/dw/tests/test_sql_skeleton.py"],
}


def _run_pytest_suite(paths: List[str]) -> Dict[str, Any]:
    """Execute pytest for the provided file list and return a structured report."""

    cmd = [sys.executable, "-m", "pytest", "-q", *paths]
    completed = subprocess.run(
        cmd,
        cwd=str(_REPO_ROOT),
        capture_output=True,
        text=True,
    )
    return {
        "command": " ".join(cmd),
        "returncode": completed.returncode,
        "stdout": (completed.stdout or "").strip(),
        "stderr": (completed.stderr or "").strip(),
    }


@golden_bp.route("/run_golden", methods=["POST"])
def run_golden():
    req = request.get_json(silent=True) or {}
    ns = req.get("namespace") or req.get("ns")  # accept both keys
    limit = req.get("limit")
    file_path = req.get("file")

    report = run_golden_tests(
        flask_app=current_app,
        namespace=ns,
        limit=limit,
        path=file_path,
    )
    if not report.get("ok", True):
        report.setdefault("error", "Golden YAML failed to load or contained no matching cases.")
        report.setdefault("namespace", ns)
        return jsonify(report), 400
    return jsonify(report), 200


tests_bp = Blueprint("dw_tests", __name__)


@tests_bp.route("/dw/test/rate", methods=["POST"])
def rate_test():
    payload = request.get_json(force=True) or {}
    # Forward to the real /dw/rate endpoint in explain-only mode so we can
    # reuse the production SQL builder without executing any database calls.
    client = current_app.test_client()
    response = client.post("/dw/rate?explain_only=1&no_retry=1", json=payload)
    try:
        data = response.get_json(force=True, silent=True) or {}
    except Exception:
        data = {}
    return jsonify(data), response.status_code


# --- Core NLU mini-suite (via HTTP) ---


@golden_bp.route("/tests/core_nlu", methods=["POST"])
def run_core_nlu():
    """Run a minimal set of core NLU parse/time checks and return a JSON report.

    This mirrors tests/test_core_nlu_parse.py so it can be executed via Postman.
    """
    results: List[Dict[str, Any]] = []
    ok_all = True

    # Case 1: parse_intent — top stakeholders by gross value
    try:
        if _parse_intent is None:
            raise RuntimeError("core.nlu.parse not available")
        question = "Top five stakeholders by gross value"
        intent = _parse_intent(question, default_date_col="REQUEST_DATE", select_all_default=True)
        checks = [
            (intent.group_by == "CONTRACT_STAKEHOLDER_1", "group_by"),
            (intent.top_n == 5, "top_n"),
            (bool(intent.user_requested_top_n), "user_requested_top_n"),
            (bool(intent.measure_sql) and "VAT" in intent.measure_sql, "measure_sql"),
            (intent.sort_by == intent.measure_sql, "sort_by_measure"),
            (intent.wants_all_columns is False, "wants_all_columns"),
            (intent.notes.get("q") == question, "notes.q"),
        ]
        ok = all(flag for flag, _ in checks)
        if not ok:
            ok_all = False
        results.append({
            "name": "core.parse_intent.top_stakeholders",
            "ok": ok,
            "failed": [name for flag, name in checks if not flag],
        })
    except Exception as exc:
        ok_all = False
        results.append({"name": "core.parse_intent.top_stakeholders", "ok": False, "error": str(exc)})

    # Case 2: resolve_window — last quarter (fixed now date)
    try:
        if _resolve_window is None:
            raise RuntimeError("core.nlu.time not available")
        w = _resolve_window("last quarter", now=date(2024, 5, 10))  # type: ignore[name-defined]
        ok = bool(w and getattr(w, "start", None) == "2024-01-01" and getattr(w, "end", None) == "2024-03-31")
        if not ok:
            ok_all = False
        results.append({
            "name": "core.resolve_window.last_quarter",
            "ok": ok,
            "got": None if not w else {"start": getattr(w, "start", None), "end": getattr(w, "end", None)},
        })
    except Exception as exc:
        ok_all = False
        results.append({"name": "core.resolve_window.last_quarter", "ok": False, "error": str(exc)})

    # Case 3: resolve_window — next 10 days (fixed now date)
    try:
        if _resolve_window is None:
            raise RuntimeError("core.nlu.time not available")
        w = _resolve_window("next 10 days", now=date(2024, 2, 15))  # type: ignore[name-defined]
        ok = bool(w and getattr(w, "start", None) == "2024-02-15" and getattr(w, "end", None) == "2024-02-25")
        if not ok:
            ok_all = False
        results.append({
            "name": "core.resolve_window.next_10_days",
            "ok": ok,
            "got": None if not w else {"start": getattr(w, "start", None), "end": getattr(w, "end", None)},
        })
    except Exception as exc:
        ok_all = False
        results.append({"name": "core.resolve_window.next_10_days", "ok": False, "error": str(exc)})

    status = 200 if ok_all else 400
    return jsonify({"ok": ok_all, "results": results}), status


@tests_bp.route("/tests/run_alias_suite", methods=["POST"])
def run_alias_suite():
    """Run alias-related pytest files (`eq_alias`, `sql_skeleton`) via HTTP."""

    payload = request.get_json(silent=True) or {}
    requested = payload.get("tests")
    selected: List[str] = []
    invalid: List[str] = []

    if requested is None:
        selected = list(_ALIAS_TEST_SUITES.keys())
    else:
        if isinstance(requested, str):
            requested = [requested]
        for item in requested or []:
            key = str(item or "").strip().lower()
            if key in _ALIAS_TEST_SUITES:
                selected.append(key)
            else:
                invalid.append(item)

    if not selected:
        return jsonify(
            {
                "ok": False,
                "error": "No valid tests selected. Use one of: eq_alias, sql_skeleton.",
                "invalid": invalid,
            }
        ), 400

    results: List[Dict[str, Any]] = []
    ok_all = True
    for key in selected:
        paths = _ALIAS_TEST_SUITES[key]
        report = _run_pytest_suite(paths)
        report["suite"] = key
        results.append(report)
        if report["returncode"] != 0:
            ok_all = False

    response_body = {
        "ok": ok_all,
        "results": results,
        "invalid": invalid or None,
    }
    status = 200 if ok_all else 400
    return jsonify(response_body), status


# --- Aggregated test runner (via HTTP) ---


@golden_bp.route("/tests/run_all", methods=["POST"])
def run_all_tests():
    """Run key HTTP-executable suites and return a consolidated report.

    POST payload (all optional):
    - run_golden_answer: bool (default true)
    - run_rate_suite:   bool (default true)
    - run_golden_rate:  bool (default true)
    - run_core_nlu:     bool (default true)
    - namespace/ns, limit, file/path: forwarded to golden endpoints
    - level: forwarded to rate suite endpoint ("easy"|"medium"|"all")
    """
    body = request.get_json(silent=True) or {}
    want_golden = body.get("run_golden_answer", True)
    want_rate_suite = body.get("run_rate_suite", True)
    want_golden_rate = body.get("run_golden_rate", True)
    want_core = body.get("run_core_nlu", True)

    out: Dict[str, Any] = {"ok": True, "parts": {}}
    any_fail = False

    with current_app.test_client() as client:
        if want_golden:
            payload = {
                "namespace": body.get("namespace") or body.get("ns"),
                "limit": body.get("limit"),
                "file": body.get("file") or body.get("path"),
            }
            rv = client.post("/admin/run_golden", json=payload)
            data = rv.get_json(silent=True) or {}
            ok = bool(data.get("ok", rv.status_code == 200))
            any_fail = any_fail or not ok
            out["parts"]["golden_answer"] = {"status": rv.status_code, **data}

        if want_rate_suite:
            payload = {"level": body.get("level") or "all"}
            rv = client.post("/dw/tests/run_rate_suite", json=payload)
            data = rv.get_json(silent=True) or {}
            ok = bool(data.get("ok", rv.status_code == 200))
            any_fail = any_fail or not ok
            out["parts"]["rate_suite"] = {"status": rv.status_code, **data}

        if want_golden_rate:
            rv = client.post("/dw/admin/run_golden_rate", json={})
            data = rv.get_json(silent=True) or {}
            ok = bool(data.get("ok", rv.status_code == 200))
            any_fail = any_fail or not ok
            out["parts"]["golden_rate"] = {"status": rv.status_code, **data}

        if want_core:
            rv = client.post("/admin/tests/core_nlu", json={})
            data = rv.get_json(silent=True) or {}
            ok = bool(data.get("ok", rv.status_code == 200))
            any_fail = any_fail or not ok
            out["parts"]["core_nlu"] = {"status": rv.status_code, **data}

    out["ok"] = not any_fail
    status = 200 if out["ok"] else 400
    return jsonify(out), status
