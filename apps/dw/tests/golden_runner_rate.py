from __future__ import annotations

import os
from pathlib import Path

import yaml
from flask import Blueprint, current_app, jsonify

from apps.dw.logger import log


golden_rate_bp = Blueprint("golden_rate", __name__)

_DEFAULT_RATE_GOLDEN = "/var/www/longchain/apps/dw/tests/golden_rate.yaml"


@golden_rate_bp.route("/admin/run_golden_rate", methods=["POST"])
def run_golden_rate():
    path = os.getenv("DW_GOLDEN_RATE_PATH", _DEFAULT_RATE_GOLDEN)
    if not os.path.exists(path):
        fallback = Path(__file__).with_name("golden_rate.yaml")
        path = str(fallback)

    try:
        with open(path, "r", encoding="utf-8") as handle:
            data = yaml.safe_load(handle) or {}
    except Exception as exc:  # pragma: no cover - defensive fallback
        log.error(f"Failed to load golden YAML: {exc}")
        return (
            jsonify({
                "ok": False,
                "error": "Golden YAML failed to load.",
                "namespace": "dw::common",
            }),
            400,
        )

    tests = data.get("tests", []) if isinstance(data, dict) else []
    passed = 0
    results = []

    app = current_app._get_current_object()
    with app.test_client() as client:
        for case in tests:
            comment = case.get("comment", "") if isinstance(case, dict) else ""
            response = client.post(
                "/dw/rate",
                json={"inquiry_id": 1, "rating": 1, "comment": comment},
            )
            ok = response.status_code == (case.get("expect_status", 200) if isinstance(case, dict) else 200)
            body = response.get_json(silent=True) or {}
            sql = ((body.get("debug") or {}).get("final_sql") or {}).get("sql", "")
            for needle in (case.get("expect_sql_contains", []) if isinstance(case, dict) else []):
                if needle not in sql:
                    ok = False
            results.append(
                {
                    "name": case.get("name") if isinstance(case, dict) else None,
                    "ok": ok,
                    "status_code": response.status_code,
                }
            )
            if ok:
                passed += 1

    total = len(tests)
    status_code = 200 if passed == total else 400
    return (
        jsonify(
            {
                "ok": passed == total,
                "passed": passed,
                "total": total,
                "results": results,
                "namespace": "dw::common",
            }
        ),
        status_code,
    )
