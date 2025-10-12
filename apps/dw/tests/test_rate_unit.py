import pytest

flask = pytest.importorskip("flask")
from flask import Flask

from apps.dw.rating import rate_bp
from apps.dw.tests.routes import tests_bp


class _SettingsStub:
    def __init__(self, values):
        self._values = values

    def get_json(self, key, scope=None, namespace=None):
        return self._values.get(key)

    def get(self, key, scope=None, namespace=None):
        return self.get_json(key, scope=scope, namespace=namespace)


@pytest.fixture(scope="module")
def app():
    app = Flask(__name__)
    app.config["TESTING"] = True
    # Provide minimal settings for the /dw/rate builder.
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
    alias_map = {
        "DEPARTMENT": [*(f"DEPARTMENT_{i}" for i in range(1, 9)), "OWNER_DEPARTMENT"],
        "STAKEHOLDER": [f"CONTRACT_STAKEHOLDER_{i}" for i in range(1, 9)],
    }
    fts_columns = {"Contract": ["CONTRACT_SUBJECT", "ENTITY", "REPRESENTATIVE_EMAIL"]}
    app.config["SETTINGS"] = _SettingsStub(
        {
            "DW_ENUM_SYNONYMS": synonyms,
            "DW_EQ_ALIAS_COLUMNS": alias_map,
            "DW_FTS_COLUMNS": fts_columns,
        }
    )
    app.config["MEM_ENGINE"] = None
    app.register_blueprint(rate_bp, url_prefix="/dw")
    app.register_blueprint(tests_bp)
    monkeypatch.setattr("apps.dw.rate_pipeline.run_query", lambda sql, binds: [])
    return app


@pytest.fixture()
def client(app):
    return app.test_client()


def post_rate(client, comment):
    payload = {"inquiry_id": 999, "rating": 1, "comment": comment}
    return client.post("/dw/test/rate", json=payload)


def test_01_order_by_only(client):
    r = post_rate(client, "order_by: REQUEST_DATE desc;")
    j = r.get_json()
    assert j["ok"] is True and "ORDER BY REQUEST_DATE DESC" in j.get("sql", "")


def test_02_eq_request_type_synonyms(client):
    r = post_rate(client, "eq: REQUEST_TYPE = Renewal;")
    j = r.get_json()
    sql = (j.get("sql") or "").upper()
    assert "REQUEST_TYPE IN (UPPER(:EQ_0" in sql
    binds = j.get("binds") or {}
    for k in ("eq_0", "eq_1", "eq_2", "eq_3", "eq_4", "eq_5", "eq_6"):
        assert k in binds


def test_03_fts_simple(client):
    r = post_rate(client, "fts: it;")
    j = r.get_json()
    assert "%IT%" in (j.get("binds", {}).get("fts_0", "")).upper()


def test_04_alias_stakeholder_expand(client):
    r = post_rate(client, "eq: stakeholder = A or B;")
    j = r.get_json()
    sql = (j.get("sql") or "").upper()
    assert "CONTRACT_STAKEHOLDER_8" in sql


def test_05_alias_department_expand(client):
    r = post_rate(client, "eq: department = X or Y;")
    j = r.get_json()
    sql = (j.get("sql") or "").upper()
    assert "OWNER_DEPARTMENT" in sql and "DEPARTMENT_8" in sql


def test_06_full_query_like_yours(client):
    comment = (
        "fts: it or home care; "
        "eq: ENTITY = DSFH or Farabi; "
        "eq: REPRESENTATIVE_EMAIL = samer@procare-sa.com or "
        "rehab.elfwakhry@oracle.com; "
        "eq: stakeholder = Amr Taher A Maghrabi or Abdulellah Mazen Fakeeh; "
        "eq: department = AL FARABI or SUPPORT SERVICES; "
        "order_by: REQUEST_DATE desc;"
    )
    r = post_rate(client, comment)
    j = r.get_json()
    sql = (j.get("sql") or "").upper()
    assert ":FTS_0" in sql and ":FTS_1" in sql
    assert "REPRESENTATIVE_EMAIL" in sql and "ENTITY" in sql
    assert "ORDER BY REQUEST_DATE DESC" in sql
