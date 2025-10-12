from __future__ import annotations

import json

import pytest

pytest.importorskip("flask")
from flask import Flask

from apps.dw.rating import rate_bp


def _configure_settings(monkeypatch):
    alias_map = {
        "DEPARTMENT": [*(f"DEPARTMENT_{i}" for i in range(1, 9)), "OWNER_DEPARTMENT"],
        "STAKEHOLDER": [f"CONTRACT_STAKEHOLDER_{i}" for i in range(1, 9)],
    }
    synonyms = {
        "Contract.REQUEST_TYPE": {
            "renewal": {
                "equals": [
                    "Renewal",
                    "Renew",
                    "Renew Contract",
                    "Renewed",
                    "Contract Renewal",
                ],
                "prefix": ["Renew", "Extens"],
                "contains": [],
            }
        }
    }
    fts_columns = {"Contract": ["CONTRACT_SUBJECT", "REPRESENTATIVE_EMAIL"]}

    def fake_get_setting_json(key, scope=None, namespace=None):
        if key == "DW_EQ_ALIAS_COLUMNS":
            return alias_map
        if key == "DW_ENUM_SYNONYMS":
            return synonyms
        if key == "DW_FTS_COLUMNS":
            return fts_columns
        return {}

    def fake_get_setting_value(key, scope=None, namespace=None):
        if key == "DW_DATE_COLUMN":
            return "REQUEST_DATE"
        return None

    monkeypatch.setattr("apps.dw.rate_pipeline.get_setting_json", fake_get_setting_json)
    monkeypatch.setattr("apps.dw.rate_pipeline.get_setting_value", fake_get_setting_value)
    monkeypatch.setattr("apps.dw.rate_pipeline.run_query", lambda sql, binds: [])


@pytest.fixture()
def client(monkeypatch):
    _configure_settings(monkeypatch)
    app = Flask(__name__)
    app.config["TESTING"] = True
    app.register_blueprint(rate_bp, url_prefix="/dw")
    with app.test_client() as test_client:
        yield test_client


def _post_rate(client, comment: str):
    payload = {"inquiry_id": 999, "rating": 1, "comment": comment}
    return client.post("/dw/rate", data=json.dumps(payload), content_type="application/json")


def test_rate_eq_request_type(client):
    resp = _post_rate(client, "eq: REQUEST_TYPE = Renewal;")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["ok"] is True
    sql = data["debug"]["final_sql"]["sql"].upper()
    assert "ORDER BY REQUEST_DATE" in sql
    assert ":EQ_0" in sql


def test_rate_eq_department_alias(client):
    resp = _post_rate(client, "eq: DEPARTMENT = SUPPORT SERVICES;")
    assert resp.status_code == 200
    sql = resp.get_json()["debug"]["final_sql"]["sql"].upper()
    assert "DEPARTMENT_1" in sql
    assert "OWNER_DEPARTMENT" in sql
    assert " OR " in sql


def test_rate_fts_like(client):
    resp = _post_rate(client, "fts: it or home care;")
    assert resp.status_code == 200
    sql = resp.get_json()["debug"]["final_sql"]["sql"].upper()
    assert "LIKE" in sql
    assert "CONTRACT_SUBJECT" in sql or "REPRESENTATIVE_EMAIL" in sql


def test_rate_mixed_eq_fts(client):
    resp = _post_rate(
        client,
        "eq: ENTITY = DSFH or Farabi; fts: home care; order_by: REQUEST_DATE desc;",
    )
    assert resp.status_code == 200
    sql = resp.get_json()["debug"]["final_sql"]["sql"].upper()
    assert " WHERE " in sql and " AND " in sql
    assert "ORDER BY REQUEST_DATE DESC" in sql
