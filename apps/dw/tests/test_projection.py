import pathlib
import sys
import types


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


from apps.dw.app import (
    _is_simple_contract_select,
    _mentions_specific_projection,
    _rewrite_projection_to_star,
    _strip_limits,
)


def test_mentions_specific_projection():
    assert not _mentions_specific_projection("Contracts with END_DATE in next 90 days")
    assert _mentions_specific_projection(
        "Show CONTRACT_ID and CONTRACT_OWNER for next 90 days"
    )
    assert _mentions_specific_projection(
        "Need contract owner and contract id details"
    )


def test_rewrite_to_star_simple():
    sql = (
        'SELECT CONTRACT_ID, CONTRACT_OWNER FROM "Contract" '
        "WHERE END_DATE BETWEEN :date_start AND :date_end ORDER BY END_DATE"
    )
    assert _is_simple_contract_select(sql)
    out = _rewrite_projection_to_star(sql)
    assert out.startswith('SELECT * FROM "Contract"')


def test_dont_touch_cte():
    sql = 'WITH x AS (SELECT 1 FROM dual) SELECT * FROM "Contract"'
    assert not _is_simple_contract_select(sql)


def test_strip_limits():
    sql = 'SELECT * FROM "Contract" FETCH FIRST 10 ROWS ONLY'
    assert _strip_limits(sql) == 'SELECT * FROM "Contract"'
