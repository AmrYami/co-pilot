import logging
import os

import yaml
from flask import Blueprint, jsonify, request

from .golden_runner import GOLDEN_PATH, run_golden_tests


golden_bp = Blueprint("golden", __name__)


def _run(namespace: str):
    report = run_golden_tests(namespace=namespace)
    logging.info(
        f"[golden] report: total={report.get('total')} passed={report.get('passed')}"
    )
    return report


@golden_bp.route("/run_golden", methods=["POST"])
def run_golden():
    body = request.get_json(silent=True) or {}
    ns = body.get("namespace") or request.args.get("namespace") or "dw::common"
    report = _run(ns)
    return jsonify(report)


@golden_bp.route("/dw/run_golden", methods=["POST"])
def run_golden_dw_alias():
    body = request.get_json(silent=True) or {}
    ns = body.get("namespace") or request.args.get("namespace") or "dw::common"
    report = _run(ns)
    return jsonify(report)


@golden_bp.route("/golden_manifest", methods=["GET"])
def golden_manifest():
    path = GOLDEN_PATH
    ok = os.path.exists(path)
    total = 0
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
            total = len(data.get("cases") or [])
    except Exception as ex:
        return jsonify({"ok": False, "path": path, "error": str(ex)})
    return jsonify({"ok": ok, "path": path, "total": total})
