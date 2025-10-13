from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List

from flask import Blueprint, current_app, jsonify, request

rate_tests_bp = Blueprint("rate_tests_bp", __name__)


@dataclass
class Expect:
    sql_contains: List[str] = field(default_factory=list)
    binds_include: Dict[str, Any] = field(default_factory=dict)
    debug_fts_engine: str | None = None
    require_sql_equals_debug_final: bool = True


@dataclass
class Case:
    name: str
    body: Dict[str, Any]
    expect: Expect


def _run_one(case: Case) -> Dict[str, Any]:
    client = current_app.test_client()
    resp = client.post("/dw/rate", json=case.body)
    ok_http = resp.status_code == 200
    data: Dict[str, Any] = {}
    try:
        data = resp.get_json(force=True, silent=False) or {}
    except Exception:
        pass

    result: Dict[str, Any] = {
        "name": case.name,
        "http_ok": ok_http,
        "ok": False,
        "errors": [],
        "response": data,
    }

    if not ok_http:
        result["errors"].append(f"HTTP {resp.status_code}")
        return result

    sql_top = data.get("sql")
    dbg = (data.get("debug") or {})
    fin = (dbg.get("final_sql") or {})
    sql_dbg = fin.get("sql")

    if not sql_top or not sql_dbg:
        result["errors"].append("Missing sql or debug.final_sql")

    if case.expect.require_sql_equals_debug_final and sql_top != sql_dbg:
        result["errors"].append("response.sql != debug.final_sql.sql")

    for frag in case.expect.sql_contains:
        if frag not in (sql_top or ""):
            result["errors"].append(f"SQL missing fragment: {frag!r}")

    binds = data.get("binds") or ((data.get("meta") or {}).get("binds") or {})
    for key, expected in case.expect.binds_include.items():
        if key not in binds:
            result["errors"].append(f"Bind {key} not found")
            continue
        exp = str(expected)
        got = str(binds[key])
        if exp.startswith("%") or exp.endswith("%"):
            if got != exp:
                result["errors"].append(f"Bind {key} expected {exp} got {got}")
        elif got != exp:
            result["errors"].append(f"Bind {key} expected {exp} got {got}")

    if case.expect.debug_fts_engine:
        fts = dbg.get("fts") or {}
        eng = fts.get("engine")
        if eng != case.expect.debug_fts_engine:
            result["errors"].append(
                f"fts.engine expected {case.expect.debug_fts_engine} got {eng}"
            )

    result["ok"] = not result["errors"]
    return result


def _cases(level: str = "all") -> List[Case]:
    base_inquiry = 9000

    cases = [
        Case(
            name="01-order-only",
            body={
                "inquiry_id": base_inquiry + 1,
                "rating": 1,
                "comment": "order_by: REQUEST_DATE desc;",
            },
            expect=Expect(
                sql_contains=["ORDER BY REQUEST_DATE DESC"],
                debug_fts_engine="like",
            ),
        ),
        Case(
            name="02-fts-single",
            body={
                "inquiry_id": base_inquiry + 2,
                "rating": 1,
                "comment": "fts: it; order_by: REQUEST_DATE desc;",
            },
            expect=Expect(
                sql_contains=["LIKE UPPER(:fts_0)", "ORDER BY REQUEST_DATE DESC"],
                binds_include={"fts_0": "%it%"},
                debug_fts_engine="like",
            ),
        ),
        Case(
            name="03-fts-or-entity-or",
            body={
                "inquiry_id": base_inquiry + 3,
                "rating": 1,
                "comment": "fts: it or home care; eq: ENTITY = DSFH or Farabi; order_by: REQUEST_DATE desc;",
            },
            expect=Expect(
                sql_contains=[
                    "LIKE UPPER(:fts_0)",
                    "LIKE UPPER(:fts_1)",
                    "ORDER BY REQUEST_DATE DESC",
                ],
                binds_include={
                    "fts_0": "%it%",
                    "fts_1": "%home care%",
                    "eq_0": "DSFH",
                    "eq_1": "Farabi",
                },
                debug_fts_engine="like",
            ),
        ),
        Case(
            name="04-full-scenario",
            body={
                "inquiry_id": base_inquiry + 4,
                "rating": 1,
                "comment": (
                    "fts: it or home care; "
                    "eq: ENTITY = DSFH or Farabi; "
                    "eq: REPRESENTATIVE_EMAIL = samer@procare-sa.com or "
                    "rehab.elfwakhry@oracle.com; "
                    "eq: stakeholder = Amr Taher A Maghrabi or Abdulellah Mazen Fakeeh; "
                    "eq: department = AL FARABI or SUPPORT SERVICES; "
                    "order_by: REQUEST_DATE desc;"
                ),
            },
            expect=Expect(
                sql_contains=[
                    "LIKE UPPER(:fts_0)",
                    "LIKE UPPER(:fts_1)",
                    "ORDER BY REQUEST_DATE DESC",
                ],
                binds_include={
                    "fts_0": "%it%",
                    "fts_1": "%home care%",
                    "eq_0": "DSFH",
                    "eq_1": "Farabi",
                    "eq_2": "samer@procare-sa.com",
                    "eq_3": "rehab.elfwakhry@oracle.com",
                    "eq_4": "Amr Taher A Maghrabi",
                    "eq_5": "Abdulellah Mazen Fakeeh",
                    "eq_6": "AL FARABI",
                    "eq_7": "SUPPORT SERVICES",
                },
                debug_fts_engine="like",
            ),
        ),
    ]

    level = level.lower()
    if level == "easy":
        return cases[:2]
    if level == "medium":
        return cases[:3]
    return cases


@rate_tests_bp.route("/run_rate_suite", methods=["POST"])
def run_rate_suite() -> Any:
    payload = request.get_json(silent=True) or {}
    level = (payload.get("level") or "all").lower()
    cases = _cases(level)
    results = [_run_one(case) for case in cases]
    passed = sum(1 for result in results if result["ok"])
    return jsonify(
        {
            "namespace": "dw::common",
            "suite": "rate",
            "level": level,
            "ok": passed == len(results),
            "passed": passed,
            "total": len(results),
            "results": results,
        }
    )
