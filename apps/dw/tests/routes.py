from __future__ import annotations

from flask import Blueprint, jsonify, request

from .golden_runner import run_golden_tests


tests_bp = Blueprint("dw_tests", __name__)
golden_bp = Blueprint("dw_golden", __name__)


@tests_bp.route("/dw/tests/run_golden", methods=["GET", "POST"])
def run_golden_via_tests_blueprint():
    ns = request.args.get("namespace") or (request.json.get("namespace") if request.is_json else None)
    report = run_golden_tests(namespace=ns or "dw::common")
    return jsonify(report)


@golden_bp.route("/admin/run_golden", methods=["POST", "GET"])
def run_golden():
    ns = request.args.get("namespace") or (request.json.get("namespace") if request.is_json else None)
    report = run_golden_tests(namespace=ns or "dw::common")
    return jsonify(report)
