from flask import Blueprint, jsonify, request

from .golden_runner import run_golden_tests


tests_bp = Blueprint("dw_tests", __name__, url_prefix="")


@tests_bp.route("/admin/run_golden", methods=["POST"])
def run_golden():
    payload = request.get_json(silent=True) or {}
    ns = payload.get("namespace") or "dw::common"
    report = run_golden_tests(namespace=ns)
    return jsonify(report), 200


@tests_bp.route("/dw/run_golden", methods=["POST"])
def run_golden_dw_alias():
    payload = request.get_json(silent=True) or {}
    ns = payload.get("namespace") or "dw::common"
    report = run_golden_tests(namespace=ns)
    return jsonify(report), 200
