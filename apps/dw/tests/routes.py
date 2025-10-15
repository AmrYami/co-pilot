from __future__ import annotations

from flask import Blueprint, current_app, jsonify, request

from .golden_runner import run_golden_tests


golden_bp = Blueprint("golden", __name__, url_prefix="/admin")


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
