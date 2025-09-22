import pathlib
import sys
import types
from datetime import date

ROOT = pathlib.Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))


flask_stub = types.ModuleType("flask")


class _Blueprint:
    def __init__(self, *args, **kwargs):
        pass

    def route(self, *args, **kwargs):
        def decorator(fn):
            return fn

        return decorator


flask_stub.Blueprint = _Blueprint
flask_stub.jsonify = lambda *args, **kwargs: {}
flask_stub.request = types.SimpleNamespace(args={}, json=None)
sys.modules.setdefault("flask", flask_stub)

sqlalchemy_stub = types.ModuleType("sqlalchemy")
sqlalchemy_stub.text = lambda sql: sql
sqlalchemy_stub.create_engine = lambda *args, **kwargs: None
sys.modules.setdefault("sqlalchemy", sqlalchemy_stub)
sqlalchemy_engine_stub = types.ModuleType("sqlalchemy.engine")
sqlalchemy_engine_stub.Engine = object
sys.modules.setdefault("sqlalchemy.engine", sqlalchemy_engine_stub)

torch_stub = types.ModuleType("torch")
torch_stub.float16 = "float16"
torch_stub.float32 = "float32"
torch_stub.float8 = "float8"
torch_stub.device = lambda device: device
torch_stub.cuda = types.SimpleNamespace(is_available=lambda: False)
sys.modules.setdefault("torch", torch_stub)

sqlglot_stub = types.ModuleType("sqlglot")
sqlglot_stub.parse_one = lambda sql, read=None: None
sqlglot_stub.exp = types.SimpleNamespace()
sys.modules.setdefault("sqlglot", sqlglot_stub)
sys.modules.setdefault("sqlglot.exp", sqlglot_stub.exp)


from apps.dw import app as app_module  # noqa: E402  # pylint: disable=wrong-import-position


def _freeze_today(monkeypatch, target: date) -> None:
    class _FakeDate(date):
        @classmethod
        def today(cls) -> "_FakeDate":
            return cls(target.year, target.month, target.day)

    monkeypatch.setattr(app_module, "date", _FakeDate)


def test_parse_clarifier_fallback():
    raw = "<<JSON>>{}<</JSON>>\nAnswer:\n{\"date_column\": \"END_DATE\", \"has_time_window\": true}"
    parsed = app_module._parse_clarifier_output(raw)
    assert parsed["date_column"] == "END_DATE"
    assert parsed["has_time_window"] is True


def test_normalize_intent_count_next_days(monkeypatch):
    _freeze_today(monkeypatch, date(2023, 1, 1))
    parsed = {"has_time_window": None, "date_column": None}
    intent = app_module._normalize_intent("contracts expiring in 30 days (count)", parsed)
    assert intent["agg"] == "count"
    assert intent["wants_all_columns"] is False
    assert intent["date_column"] == "END_DATE"
    assert intent["has_time_window"] is True
    assert intent["explicit_dates"] == {"start": "2023-01-01", "end": "2023-01-31"}


def test_normalize_intent_next_90_days(monkeypatch):
    _freeze_today(monkeypatch, date(2023, 5, 15))
    intent = app_module._normalize_intent(
        "Contracts with END_DATE in the next 90 days.",
        {},
    )
    assert intent["date_column"] == "END_DATE"
    assert intent["has_time_window"] is True
    assert intent["wants_all_columns"] is True
    assert intent["explicit_dates"] == {"start": "2023-05-15", "end": "2023-08-13"}


def test_normalize_intent_top_value_last_month(monkeypatch):
    _freeze_today(monkeypatch, date(2023, 2, 10))
    intent = app_module._normalize_intent("top 10 stakeholders by contract value last month", {})
    assert intent["top_n"] == 10
    assert intent["sort_by"] == "CONTRACT_VALUE_NET_OF_VAT"
    assert intent["sort_desc"] is True
    assert intent["explicit_dates"] == {"start": "2023-01-01", "end": "2023-01-31"}


def test_maybe_rewrite_sql_for_count():
    sql = 'SELECT CONTRACT_ID FROM "Contract" WHERE END_DATE BETWEEN :date_start AND :date_end'
    rewritten, meta, _ = app_module._maybe_rewrite_sql_for_intent(sql, {"agg": "count"})
    assert "COUNT(*)" in rewritten.upper()
    assert meta["used_projection_rewrite"] is True


def test_maybe_rewrite_sql_for_top_n():
    sql = 'SELECT * FROM "Contract"'
    intent = {
        "top_n": 10,
        "sort_by": "CONTRACT_VALUE_NET_OF_VAT",
        "sort_desc": True,
        "user_requested_top_n": True,
    }
    rewritten, meta, _ = app_module._maybe_rewrite_sql_for_intent(sql, intent)
    assert "ORDER BY CONTRACT_VALUE_NET_OF_VAT DESC" in rewritten
    assert "FETCH FIRST :top_n ROWS ONLY" in rewritten
    assert meta["used_limit_inject"] is True
    assert meta["used_order_inject"] is True


def test_maybe_rewrite_sql_skips_limit_when_not_requested():
    sql = 'SELECT * FROM "Contract"'
    intent = {
        "top_n": 10,
        "sort_by": "CONTRACT_VALUE_NET_OF_VAT",
        "sort_desc": True,
        "user_requested_top_n": False,
    }
    rewritten, meta, _ = app_module._maybe_rewrite_sql_for_intent(sql, intent)
    assert "ORDER BY CONTRACT_VALUE_NET_OF_VAT DESC" in rewritten
    assert "FETCH FIRST :top_n ROWS ONLY" not in rewritten
    assert meta["used_limit_inject"] is False
    assert meta["used_order_inject"] is True
