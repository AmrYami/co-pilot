# apps/dw/tests/routes.py
from __future__ import annotations
from flask import Blueprint, current_app, jsonify, request
from .golden_runner import run_golden_tests

golden_bp = Blueprint("golden", __name__, url_prefix="/admin")

@golden_bp.route("/run_golden", methods=["POST"])
def run_golden():
    req = request.get_json(silent=True) or {}
    ns = req.get("namespace") or req.get("ns")  # accept both keys
    limit = req.get("limit")

    report = run_golden_tests(flask_app=current_app, namespace=ns, limit=limit)
    return jsonify(report), 200
