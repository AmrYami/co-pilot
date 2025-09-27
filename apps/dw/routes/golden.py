from __future__ import annotations

import os
from pathlib import Path

from flask import Blueprint, current_app, jsonify, request

from apps.dw.tests.golden import run_golden

bp_golden = Blueprint("dw_golden", __name__, url_prefix="/dw")


@bp_golden.route("/run_golden", methods=["POST"])
def run_golden_route():
    admin_key = os.environ.get("SETTINGS_ADMIN_KEY")
    if current_app:
        admin_key = current_app.config.get("SETTINGS_ADMIN_KEY", admin_key)
    provided = request.headers.get("X-Admin-Key")
    if admin_key and provided != admin_key:
        return jsonify({"ok": False, "error": "forbidden"}), 403

    fp = Path("apps/dw/tests/golden_dw_contracts.yaml")
    if not fp.exists():
        return jsonify({"ok": False, "error": "golden file missing"}), 404

    results = run_golden(str(fp))
    summary = {
        "total": len(results),
        "passed": sum(1 for r in results if r["ok"]),
        "failed": sum(1 for r in results if not r["ok"]),
    }
    return jsonify({"ok": True, "summary": summary, "results": results})
