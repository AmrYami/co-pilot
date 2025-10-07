from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable

import pytest

try:  # pragma: no cover - optional dependency for YAML parsing
    import yaml
except ModuleNotFoundError:  # pragma: no cover - fallback when PyYAML unavailable
    yaml = None  # type: ignore[assignment]

pytest.importorskip("flask")

from longchain import app as flask_app


@pytest.fixture()
def client():
    if flask_app is None:
        pytest.skip("Flask application is unavailable")
    return flask_app.test_client()


def _contains_all(text: str, fragments: Iterable[str]) -> bool:
    body = text or ""
    return all(fragment in body for fragment in fragments if fragment)


def _contains_none(text: str, fragments: Iterable[str]) -> bool:
    body = text or ""
    return all(fragment not in body for fragment in fragments if fragment)


def test_explain_contains_fts_and_order(client):
    payload = {
        "prefixes": [],
        "question": "list all contracts has it or home care",
        "auth_email": "dev@example.com",
        "full_text_search": True,
    }
    rv = client.post("/dw/answer", json=payload)
    assert rv.status_code == 200
    data = rv.get_json()
    assert data["ok"] is True
    meta = data["meta"]
    assert "Fallback listing" not in (meta.get("explain") or "")
    assert "FTS(" in meta.get("user_explain", "")
    assert "ORDER BY REQUEST_DATE DESC" in data["sql"]


def test_explain_struct_populated(client):
    payload = {
        "prefixes": [],
        "question": "show contracts where REQUEST TYPE = Renewal",
        "auth_email": "dev@example.com",
    }
    rv = client.post("/dw/answer", json=payload)
    assert rv.status_code == 200
    data = rv.get_json()
    struct = data["debug"].get("explain_struct")
    assert struct is not None
    assert struct["order_by"]["column"]
    assert struct["order_by"]["desc"] is True
    assert data["explain"].startswith("Applied equality filters")


def test_rate_applies_eq_and_fts(client):
    rv = client.post(
        "/dw/answer",
        json={
            "prefixes": [],
            "question": "list all contracts has it or home care and ENTITY = DSFH",
            "auth_email": "dev@example.com",
            "full_text_search": True,
        },
    )
    ans = rv.get_json()
    inq = ans["inquiry_id"]

    rv2 = client.post(
        "/dw/rate",
        json={
            "inquiry_id": inq,
            "rating": 1,
            "comment": "fts: it | home care; eq: ENTITY = DSFH; order_by: REQUEST_DATE desc;",
        },
    )
    fix = rv2.get_json()
    sql = fix.get("sql", "")
    assert "LIKE UPPER(:fts_0)" in sql and "LIKE UPPER(:fts_1)" in sql
    assert "UPPER(TRIM(ENTITY))" in sql or "ENTITY = :eq_0" in sql
    assert "ORDER BY REQUEST_DATE DESC" in sql


def test_admin_explain_renders(client):
    base = client.post(
        "/dw/answer",
        json={
            "prefixes": [],
            "question": "Total gross per DEPARTMENT_OUL",
            "auth_email": "dev@example.com",
        },
    ).get_json()

    rv = client.post("/dw/admin/explain", json=base)
    assert rv.status_code == 200
    text = rv.get_data(as_text=True)
    assert "Explain — Interpretation" in text
    assert "GROUP BY" in text


def test_golden_cases_from_yaml(client):
    if yaml is None:
        pytest.skip("PyYAML is required for golden YAML parsing")

    yaml_path = Path(__file__).with_name("golden_p2_explain_fts.yaml")
    cases = yaml.safe_load(yaml_path.read_text(encoding="utf-8")) or []
    assert cases, "Golden YAML should contain cases"

    for case in cases:
        request_cfg: Dict[str, Any] = case.get("request") or {}
        endpoint = request_cfg.get("endpoint") or "/dw/answer"
        body = request_cfg.get("body") or {}
        rv = client.post(endpoint, json=body)
        assert rv.status_code == 200, f"Request failed for {case.get('name')}"
        data = rv.get_json()

        sql = data.get("sql", "")
        explain_text = data.get("explain", "") or data.get("meta", {}).get("explain", "")

        expect: Dict[str, Any] = case.get("expect") or {}
        must = expect.get("must_contain") or []
        assert _contains_all(sql, must), f"Missing SQL fragments for {case.get('name')}: {must}"

        must_not = expect.get("must_not_contain") or []
        assert _contains_none(sql, must_not), f"Forbidden SQL fragment in {case.get('name')}: {must_not}"

        order_col = expect.get("require_order_by")
        if order_col:
            assert "ORDER BY" in sql.upper()
            assert order_col.upper() in sql.upper()

        order_dir = expect.get("require_order_dir")
        if order_dir:
            assert order_dir.upper() in sql.upper()

        group_col = expect.get("require_group_by")
        if group_col:
            assert "GROUP BY" in sql.upper()
            assert group_col.upper() in sql.upper()

        explain_frags = expect.get("require_explain_contains") or []
        assert _contains_all(explain_text, explain_frags), (
            f"Explain missing fragments for {case.get('name')}: {explain_frags}"
        )

        struct = data.get("debug", {}).get("explain_struct")
        assert struct is not None, "Explain struct should be present"
