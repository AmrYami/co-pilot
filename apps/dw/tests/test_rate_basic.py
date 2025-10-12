"""Basic behaviour tests for the /dw/rate endpoint."""

import pytest

pytest.importorskip("flask")
from flask import Flask

from apps.dw.rating import rate_bp


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
        "contains": [],
    }
}


class _SettingsStub:
    def __init__(self, values):
        self._values = values

    def get_json(self, key, scope=None, namespace=None):  # pragma: no cover - simple stub
        return self._values.get(key)

    def get(self, key, scope=None, namespace=None):  # pragma: no cover - simple stub
        return self._values.get(key)


@pytest.fixture()
def client(monkeypatch):
    monkeypatch.setenv("DW_RATE_ENABLE_ALT_PLAN", "0")
    monkeypatch.setenv("DW_RATE_EXECUTE_FINAL", "0")

    app = Flask(__name__)
    app.config["TESTING"] = True
    synonyms = {"Contract.REQUEST_TYPE": REQUEST_TYPE_SYNONYMS}
    app.config["SETTINGS"] = _SettingsStub({"DW_ENUM_SYNONYMS": synonyms})
    app.config["MEM_ENGINE"] = None
    app.register_blueprint(rate_bp, url_prefix="/dw")

    with app.test_client() as test_client:
        yield test_client


def _post_rate(client, payload):
    return client.post("/dw/rate", json=payload)


def test_rate_eq_request_type_simple(client):
    payload = {
        "inquiry_id": 27,
        "rating": 1,
        "comment": "eq: REQUEST_TYPE = Renewal;",
    }
    resp = _post_rate(client, payload)
    assert resp.status_code == 200
    data = resp.get_json()
    assert data.get("retry") is False
    dbg = data["debug"]
    sql = dbg["final_sql"]["sql"]
    assert "UPPER(TRIM(REQUEST_TYPE))" in sql
    assert "= UPPER(:eq_0)" in sql or "= UPPER(:eq_1)" in sql
    validation = dbg["validation"]
    assert "bind_names" in validation and len(validation["bind_names"]) >= 1


def test_rate_fts_and_eq_combo(client):
    payload = {
        "inquiry_id": 18,
        "rating": 1,
        "comment": (
            "fts: it or home care; "
            "eq: ENTITY = DSFH or Farabi; "
            "order_by: REQUEST_DATE desc;"
        ),
    }
    resp = _post_rate(client, payload)
    assert resp.status_code == 200
    data = resp.get_json()
    assert data.get("retry") is False
    sql = data["debug"]["final_sql"]["sql"]
    assert "LIKE UPPER(:fts_" in sql or "ILIKE" in sql
    assert "ENTITY" in sql and ":eq_0" in sql
