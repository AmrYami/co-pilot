import pathlib
import sys
import types

sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))

if "pydantic" not in sys.modules:  # pragma: no cover - lightweight stub for tests
    pydantic_stub = types.ModuleType("pydantic")

    class _BaseModel:  # pragma: no cover - simple stand-in
        pass

    def _Field(*args, **kwargs):  # pragma: no cover
        return None

    pydantic_stub.BaseModel = _BaseModel
    pydantic_stub.Field = _Field
    sys.modules["pydantic"] = pydantic_stub

if "word2number" not in sys.modules:  # pragma: no cover
    w2n_module = types.ModuleType("word2number")

    class _W2N:  # pragma: no cover
        @staticmethod
        def word_to_num(text):
            raise ValueError("stub")

    w2n_module.w2n = _W2N()
    sys.modules["word2number"] = w2n_module

if "dateutil" not in sys.modules:  # pragma: no cover
    dateutil_module = types.ModuleType("dateutil")
    relativedelta_module = types.ModuleType("dateutil.relativedelta")

    class _Relativedelta:  # pragma: no cover
        def __init__(self, **kwargs):
            self.kwargs = kwargs

    relativedelta_module.relativedelta = _Relativedelta
    dateutil_module.relativedelta = relativedelta_module
    sys.modules["dateutil"] = dateutil_module
    sys.modules["dateutil.relativedelta"] = relativedelta_module

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

from apps.dw.nlp import extract_equalities_first
from apps.dw.fts import build_fts_tokens, build_like_fts_where
from apps.dw import filters as filters_mod
from apps.dw.filters import eq_filters_to_where, request_type_synonyms
from apps.dw.intent import derive_intent
from apps.dw.rate_grammar import apply_rate_comment
from apps.dw.sql_builder import build_contract_sql
from apps.dw.routes import bp as dw_blueprint


@pytest.fixture(autouse=True)
def _patch_settings(monkeypatch):
    def fake_get_setting(key, *, scope=None, namespace=None, default=None):
        if key == "DW_FTS_COLUMNS":
            return {"Contract": ["CONTRACT_SUBJECT", "ENTITY"]}
        if key == "DW_EXPLICIT_FILTER_COLUMNS":
            return ["ENTITY", "REQUEST_TYPE"]
        if key == "DW_ENUM_SYNONYMS":
            return {
                "Contract.REQUEST_TYPE": {
                    "home care": {"equals": ["HOME CARE", "HOME HEALTH"]}
                }
            }
        return default

    monkeypatch.setattr(filters_mod, "get_setting", fake_get_setting)
    monkeypatch.setattr("apps.dw.fts._get_setting", lambda *args, **kwargs: fake_get_setting("DW_FTS_COLUMNS"))
    yield


def test_extract_equalities_first():
    cleaned, pairs = extract_equalities_first('Entity = "DSFH" and home care services')
    assert pairs == [("Entity", "DSFH")]
    assert "DSFH" not in cleaned
    assert "home care" in cleaned.lower()


def test_build_like_fts_where(monkeypatch):
    groups = build_fts_tokens("home care or hospital and urgent")
    assert groups == [["home care"], ["hospital", "urgent"]]

    where_sql, binds = build_like_fts_where("Contract", groups, bind_prefix="b")
    assert "UPPER(NVL(CONTRACT_SUBJECT,''))" in where_sql
    assert binds == {"b_0": "%home care%", "b_1": "%hospital%", "b_2": "%urgent%"}


def test_eq_filters_to_where_and_synonyms():
    filters = [
        {"col": "entity", "val": "DSFH", "ci": True, "trim": True},
        {"col": "REQUEST_TYPE", "val": "home care", "ci": True, "trim": True},
    ]
    where_sql, binds = eq_filters_to_where(filters)
    assert "UPPER(TRIM(ENTITY))" in where_sql
    assert "eq_0" in binds and binds["eq_0"] == "DSFH"

    synonyms = request_type_synonyms(["home care", "other"])
    assert synonyms == ["HOME CARE", "HOME HEALTH", "OTHER"]


def test_derive_intent_and_sql_builder():
    payload = {"question": 'Entity = "DSFH" has home care', "full_text_search": True}
    intent = derive_intent(payload)
    assert intent["eq_filters"] == [{"col": "Entity", "val": "DSFH", "ci": True, "trim": True}]
    assert intent["fts"]["enabled"]
    sql, binds = build_contract_sql(intent)
    assert "FROM \"Contract\"" in sql
    assert ":eq_0" in sql
    assert binds["eq_0"] == "DSFH"


def test_apply_rate_comment(monkeypatch):
    base_intent = {"schema_key": "Contract", "eq_filters": []}
    comment = "eq: ENTITY = DSFH (ci, trim); fts: urgent | care; group_by: REQUEST_TYPE; gross: true; order_by: REQUEST_DATE asc"
    patched = apply_rate_comment(base_intent, comment)
    assert patched["eq_filters"][-1]["val"] == "DSFH"
    assert patched["fts"]["enabled"]
    assert patched["group_by"] == ["REQUEST_TYPE"]
    assert patched["gross"] is True
    assert patched["sort_desc"] is False

    sql, binds = build_contract_sql(patched)
    assert "GROUP BY REQUEST_TYPE" in sql
    assert "SUM(" in sql
    assert "ORDER BY MEASURE ASC" in sql
    assert binds


def test_routes_endpoints(monkeypatch):
    if Flask is None:
        pytest.skip("Flask is required for route tests")

    monkeypatch.setattr("apps.dw.routes.fetch_rows", lambda sql, binds: [{"sql": sql, "binds": binds}])

    app = Flask(__name__)
    app.register_blueprint(dw_blueprint)

    client = app.test_client()

    resp = client.post("/dw/answer", json={"question": "Entity = DSFH", "full_text_search": True})
    data = resp.get_json()
    assert resp.status_code == 200
    assert data["ok"] is True
    assert data["rows"][0]["binds"]["eq_0"] == "DSFH"

    resp = client.post("/dw/rate", json={"comment": "eq: ENTITY = DSFH"})
    data = resp.get_json()
    assert resp.status_code == 200
    assert data["ok"] is True
    assert data["debug"]["validation"]["ok"] is True
