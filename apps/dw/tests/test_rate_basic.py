import pathlib
import sys

import pytest

pytest.importorskip("flask")
from flask import Flask

ROOT = pathlib.Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

import apps.dw.routes.__init__ as dw_routes

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


@pytest.fixture()
def client(monkeypatch):
    def fake_get_setting(key, *, scope=None, namespace=None, default=None):
        if key == "DW_ENUM_SYNONYMS":
            return {"Contract.REQUEST_TYPE": REQUEST_TYPE_SYNONYMS}
        return default

    monkeypatch.setattr(dw_routes, "_get_setting", fake_get_setting)
    monkeypatch.setenv("DW_RATE_DISABLE_ALT_RETRY", "1")
    monkeypatch.setenv("VALIDATE_WITH_EXPLAIN_ONLY", "1")

    app = Flask(__name__)
    app.register_blueprint(dw_routes.bp)

    with app.test_client() as test_client:
        yield test_client


def _post_rate(client, comment):
    payload = {"inquiry_id": 27, "rating": 1, "comment": comment}
    return client.post("/dw/rate", json=payload)


def test_rate_eq_request_type_renewal_builds_ci_sql(client):
    response = _post_rate(client, "eq: REQUEST_TYPE = Renewal;")
    assert response.status_code == 200
    data = response.get_json()
    sql = data["debug"]["final_sql"]["sql"]
    assert "UPPER(TRIM(REQUEST_TYPE))" in sql
    assert "LIKE" in sql
    assert data.get("retry") in (False, None)


def test_rate_complex_filters_sql_only(client):
    comment = (
        "fts: it or home care; "
        "eq: ENTITY = DSFH or Farabi; "
        "eq: REPRESENTATIVE_EMAIL = samer@procare-sa.com or rehab.elfwakhry@oracle.com; "
        "eq: stakeholder = Amr Taher A Maghrabi or Abdulellah Mazen Fakeeh; "
        "eq: department = AL FARABI or SUPPORT SERVICES; "
        "order_by: REQUEST_DATE desc;"
    )
    response = _post_rate(client, comment)
    data = response.get_json()
    sql = data["debug"]["final_sql"]["sql"]
    assert ":fts_0" in sql and ":fts_1" in sql
    assert "CONTRACT_STAKEHOLDER" in sql or "STAKEHOLDER" in sql
    assert "DEPARTMENT" in sql
    assert data.get("retry") in (False, None)
