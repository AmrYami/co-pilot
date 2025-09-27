from flask import Blueprint, jsonify, request, current_app

from .golden_runner import run_golden_tests


golden_bp = Blueprint("golden", __name__)


@golden_bp.route("/admin/run_golden", methods=["POST"])
def run_golden():
    payload = request.get_json(silent=True) or {}
    ns = payload.get("namespace") or "dw::common"
    # Ensure we are inside app context (should already be, but defensive)
    with current_app.app_context():
        report = run_golden_tests(namespace=ns)
    return jsonify(report)
