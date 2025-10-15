from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

pytest.importorskip("flask")

from flask import Flask

from apps.dw.routes import bp as dw_bp


REQUEST_TYPE_SYNONYMS = {
    "renewal": {
        "equals": [
            "Renewal",
            "Renew",
            "Renew Contract",
            "Renewed",
            "Contract Renewal",
        ],
        "prefix": ["Renew", "Extens"],
        "contains": ["Extension"],
    }
}


@pytest.fixture()
def dw_client(monkeypatch):
    monkeypatch.setenv("DW_RATE_DISABLE_ALT_RETRY", "1")

    def fake_get_setting(key, *, scope=None, namespace=None, default=None):
        if key == "DW_ENUM_SYNONYMS":
            return {"Contract.REQUEST_TYPE": REQUEST_TYPE_SYNONYMS}
        if key == "DW_EXPLICIT_FILTER_COLUMNS":
            return ["REQUEST_TYPE", "ENTITY"]
        if key == "DW_CONTRACT_TABLE":
            return "Contract"
        if key == "DW_DATE_COLUMN":
            return "REQUEST_DATE"
        if key == "DW_FTS_COLUMNS":
            return {}
        if key == "DW_EQ_ALIAS_COLUMNS":
            return {}
        if key == "DW_FTS_ENGINE":
            return "like"
        if key == "DW_FTS_MIN_TOKEN_LEN":
            return 2
        return default

    monkeypatch.setattr("apps.dw.routes._get_setting", fake_get_setting)
    monkeypatch.setattr("apps.dw.routes.fetch_rows", lambda sql, binds: [])

    app = Flask(__name__)
    app.config["TESTING"] = True
    app.register_blueprint(dw_bp)

    with app.test_client() as client:
        yield client


def test_rate_request_type_renewal(dw_client):
    resp = dw_client.post(
        "/dw/rate",
        json={
            "inquiry_id": 1,
            "rating": 1,
            "comment": "eq: REQUEST_TYPE = Renewal;",
        },
    )
    data = resp.get_json()
    sql = data["sql"]

    assert data["ok"] is True
    assert "UPPER(TRIM(REQUEST_TYPE))" in sql
    assert "ORDER BY REQUEST_DATE DESC" in sql


def test_rate_request_type_extension_prefix(dw_client):
    resp = dw_client.post(
        "/dw/rate",
        json={
            "inquiry_id": 2,
            "rating": 1,
            "comment": "eq: REQUEST_TYPE = Extens;",
        },
    )
    sql = resp.get_json()["sql"]

    assert "LIKE UPPER(:eq_" in sql


def test_rate_matches_answer_sql_shape(dw_client):
    question = {"prefixes": [], "question": "Show contracts where REQUEST TYPE = Renewal"}
    ans = dw_client.post("/dw/answer", json=question).get_json()
    rate = dw_client.post(
        "/dw/rate",
        json={
            "inquiry_id": 3,
            "rating": 1,
            "comment": "eq: REQUEST_TYPE = Renewal;",
        },
    ).get_json()

    assert "UPPER(TRIM(REQUEST_TYPE))" in ans["sql"]
    assert "UPPER(TRIM(REQUEST_TYPE))" in rate["sql"]
