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


def _settings_payload():
    alias_map = {
        "DEPARTMENT": [
            *(f"DEPARTMENT_{i}" for i in range(1, 9)),
            "OWNER_DEPARTMENT",
        ],
        "STAKEHOLDER": [f"CONTRACT_STAKEHOLDER_{i}" for i in range(1, 9)],
    }
    explicit_columns = [
        "REQUEST_TYPE",
        "ENTITY",
        "REPRESENTATIVE_EMAIL",
        *alias_map["DEPARTMENT"],
        *alias_map["STAKEHOLDER"],
    ]
    fts_columns = {
        "Contract": [
            "CONTRACT_SUBJECT",
            "ENTITY",
            "OWNER_DEPARTMENT",
            "REQUEST_TYPE",
        ]
    }
    return alias_map, explicit_columns, fts_columns


@pytest.fixture()
def app(monkeypatch):
    from apps.dw import settings as settings_mod

    alias_map, explicit_columns, fts_columns = _settings_payload()

    def fake_get_setting(key, *, scope=None, namespace=None, default=None):
        if key == "DW_ENUM_SYNONYMS":
            return {"Contract.REQUEST_TYPE": REQUEST_TYPE_SYNONYMS}
        if key == "DW_EQ_ALIAS_COLUMNS":
            return alias_map
        if key == "DW_EXPLICIT_FILTER_COLUMNS":
            return explicit_columns
        if key == "DW_FTS_COLUMNS":
            return fts_columns
        if key == "DW_FTS_ENGINE":
            return "like"
        if key == "DW_FTS_MIN_TOKEN_LEN":
            return 2
        if key == "DW_CONTRACT_TABLE":
            return "Contract"
        if key == "DW_DATE_COLUMN":
            return "REQUEST_DATE"
        return default

    def fake_get_settings():
        return {
            "DW_EQ_ALIAS_COLUMNS": alias_map,
            "DW_EXPLICIT_FILTER_COLUMNS": explicit_columns,
        }

    monkeypatch.setenv("DW_RATE_DISABLE_ALT_RETRY", "1")
    monkeypatch.setattr("apps.dw.routes._get_setting", fake_get_setting)
    monkeypatch.setattr("apps.dw.routes.fetch_rows", lambda sql, binds: [])
    monkeypatch.setattr(settings_mod, "get_settings", fake_get_settings)

    app = Flask(__name__)
    app.config["TESTING"] = True
    app.register_blueprint(dw_bp)
    return app


def _post_rate(client, inquiry_id, comment):
    payload = {"inquiry_id": inquiry_id, "rating": 1, "comment": comment}
    return client.post("/dw/rate", json=payload)


def test_rate_eq_request_type_renewal(app):
    client = app.test_client()
    response = _post_rate(client, 1, "eq: REQUEST_TYPE = Renewal;")
    data = response.get_json()

    sql = data["sql"].upper()
    assert "UPPER(TRIM(REQUEST_TYPE)) IN" in sql

    binds = data.get("binds") or data["meta"]["binds"]
    assert binds["eq_0"] == "RENEWAL"
    assert binds["eq_5"].startswith("RENEW")

    assert data["ok"] is True
    assert data["retry"] is False


def test_rate_fts_or_groups(app):
    client = app.test_client()
    response = _post_rate(client, 2, "fts: it or home care")
    data = response.get_json()

    sql = data["sql"].upper()
    assert "LIKE UPPER(:FTS_0)" in sql and "LIKE UPPER(:FTS_1)" in sql

    binds = data.get("binds") or data["meta"]["binds"]
    assert binds["fts_0"] == "%IT%"
    assert binds["fts_1"] == "%HOME CARE%"

    assert "ORDER BY REQUEST_DATE DESC" in sql


def test_rate_alias_department_and_stakeholder(app):
    client = app.test_client()
    comment = (
        "eq: department = AL FARABI or SUPPORT SERVICES; "
        "eq: stakeholder = Amr Taher A Maghrabi or Abdulellah Mazen Fakeeh;"
    )
    response = _post_rate(client, 3, comment)
    data = response.get_json()

    sql = data["sql"].upper()
    assert "DEPARTMENT_1" in sql and "OWNER_DEPARTMENT" in sql
    assert "CONTRACT_STAKEHOLDER_8" in sql
