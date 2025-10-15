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

    namespace = "dw::common"
    cases = []
    if isinstance(data, dict):
        namespace = data.get("namespace", namespace)
        cases = data.get("cases") or data.get("tests") or []

    passed = 0
    results = []

    app = current_app._get_current_object()
    with app.test_client() as client:
        for case in cases:
            if not isinstance(case, dict):
                continue

            comment = case.get("rate_comment") or case.get("comment") or ""
            response = client.post(
                "/dw/rate",
                json={"inquiry_id": 1, "rating": 1, "comment": comment},
            )
            expect = case.get("expect", {})
            expect_status = case.get("expect_status")
            if not expect_status and isinstance(expect, dict):
                expect_status = expect.get("status")
            ok = response.status_code == (expect_status or 200)
            body = response.get_json(silent=True) or {}
            sql = ((body.get("debug") or {}).get("final_sql") or {}).get("sql", "")

            sql_contains = []
            if isinstance(expect, dict):
                sql_contains = expect.get("sql_contains", []) or expect.get("sql_contains_all", [])
            for needle in sql_contains:
                if needle not in sql:
                    ok = False

            binds = body.get("binds") or {}
            expect_binds = expect.get("binds") if isinstance(expect, dict) else None
            if isinstance(expect_binds, dict):
                ok = ok and all(binds.get(k) == v for k, v in expect_binds.items())

            binds_subset = expect.get("binds_subset") if isinstance(expect, dict) else None
            if isinstance(binds_subset, dict):
                for key, value in binds_subset.items():
                    if binds.get(key) != value:
                        ok = False

            rows_min = expect.get("rows_min") if isinstance(expect, dict) else None
            if isinstance(rows_min, int):
                rows = body.get("rows") or []
                if len(rows) < rows_min:
                    ok = False

            results.append(
                {
                    "name": case.get("name"),
                    "ok": ok,
                    "status_code": response.status_code,
                }
            )
            if ok:
                passed += 1

    total = len(results)
    status_code = 200 if passed == total else 400
    return (
        jsonify(
            {
                "ok": passed == total,
                "passed": passed,
                "total": total,
                "results": results,
                "namespace": namespace,
            }
        ),
        status_code,
    )
