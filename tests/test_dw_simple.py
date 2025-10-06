import sys
import types
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

import pytest

try:
    from flask import Flask
except ModuleNotFoundError:  # pragma: no cover - optional dependency
    Flask = None  # type: ignore

    flask_stub = types.ModuleType("flask")

    class _Blueprint:  # pragma: no cover
        def __init__(self, name, import_name):
            self.name = name
            self.import_name = import_name

        def route(self, rule, methods=None):  # pragma: no cover
            def decorator(func):
                return func

            return decorator

    def _jsonify(obj):  # pragma: no cover
        return obj

    class _Request:  # pragma: no cover
        def get_json(self, *args, **kwargs):
            return {}

    flask_stub.Blueprint = _Blueprint
    flask_stub.jsonify = _jsonify
    flask_stub.request = _Request()
    flask_stub.Flask = None  # type: ignore
    sys.modules.setdefault("flask", flask_stub)

from apps.dw import filters as filters_mod
from apps.dw.eq_parser import extract_eq_filters_from_natural_text
from apps.dw.fts_like import build_fts_where
from apps.dw.rate_grammar import parse_rate_comment
from apps.dw.routes import bp as dw_blueprint


@pytest.fixture(autouse=True)
def _patch_settings(monkeypatch):
    def fake_get_setting(key, *, scope=None, namespace=None, default=None):
        if key == "DW_FTS_COLUMNS":
            return {"Contract": ["CONTRACT_SUBJECT", "ENTITY"]}
        if key == "DW_EXPLICIT_FILTER_COLUMNS":
            return ["ENTITY", "REQUEST_TYPE", "REPRESENTATIVE_EMAIL"]
        if key == "DW_ENUM_SYNONYMS":
            return {
                "Contract.REQUEST_TYPE": {
                    "renewal": {
                        "equals": ["RENEWAL"],
                        "prefix": ["RENEW"],
                        "contains": ["REN"],
                    }
                }
            }
        return default

    monkeypatch.setattr(filters_mod, "get_setting", fake_get_setting)
    monkeypatch.setattr("apps.dw.routes._get_setting", fake_get_setting)
    yield


def test_extract_eq_filters_from_natural_text():
    cols = ["ENTITY", "REQUEST_TYPE", "REPRESENTATIVE_EMAIL"]
    pairs = extract_eq_filters_from_natural_text(
        "Show contracts where entity = DSFH and request type equals Renewal",
        cols,
    )
    assert ("ENTITY", "DSFH") in pairs
    assert any(col == "REQUEST_TYPE" and val.lower() == "renewal" for col, val in pairs)


def test_build_fts_where_groups():
    groups = [["it"], ["home care"]]
    sql, binds = build_fts_where(groups, ["CONTRACT_SUBJECT", "ENTITY"], "OR")
    assert "UPPER(NVL(CONTRACT_SUBJECT,''))" in sql
    assert binds == {"fts_0": "%it%", "fts_1": "%home care%"}


def test_parse_rate_comment_with_flags():
    comment = "fts: it | home care; eq: ENTITY = DSFH (ci, trim); order_by: REQUEST_DATE asc; group_by: REQUEST_TYPE; gross: true"
    hints = parse_rate_comment(comment)
    assert hints["fts_tokens"] == ["it", "home care"]
    assert hints["eq_filters"][0]["ci"] is True
    assert hints["eq_filters"][0]["trim"] is True
    assert hints["group_by"] == ["REQUEST_TYPE"]
    assert hints["sort_by"] == "REQUEST_DATE"
    assert hints["sort_desc"] is False
    assert hints["gross"] is True


@pytest.mark.skipif(Flask is None, reason="Flask is required for route tests")
def test_answer_endpoint_combines_fts_and_eq(monkeypatch):
    monkeypatch.setattr("apps.dw.routes.fetch_rows", lambda sql, binds: [{"sql": sql, "binds": binds}])

    app = Flask(__name__)
    app.register_blueprint(dw_blueprint)

    client = app.test_client()
    payload = {
        "question": "list all contracts has it or home care and entity = DSFH",
        "full_text_search": True,
    }
    resp = client.post("/dw/answer", json=payload)
    data = resp.get_json()

    assert resp.status_code == 200
    assert data["ok"] is True
    assert "UPPER(TRIM(ENTITY)) = UPPER(TRIM(:eq_0))" in data["sql"]
    assert "LIKE UPPER(:fts_0)" in data["sql"]
    assert data["debug"]["fts"]["enabled"] is True
    assert data["rows"][0]["binds"]["eq_0"].upper() == "DSFH"


@pytest.mark.skipif(Flask is None, reason="Flask is required for route tests")
def test_rate_endpoint_uses_rate_comment(monkeypatch):
    monkeypatch.setattr("apps.dw.routes.fetch_rows", lambda sql, binds: [{"sql": sql, "binds": binds}])

    app = Flask(__name__)
    app.register_blueprint(dw_blueprint)

    client = app.test_client()
    resp = client.post(
        "/dw/rate",
        json={"comment": "fts: it | home care; eq: ENTITY = DSFH; order_by: REQUEST_DATE desc"},
    )
    data = resp.get_json()

    assert resp.status_code == 200
    assert data["ok"] is True
    assert "UPPER(TRIM(ENTITY)) = UPPER(TRIM(:eq_0))" in data["sql"]
    assert "LIKE UPPER(:fts_0)" in data["sql"]
    assert data["debug"]["validation"]["ok"] is True
