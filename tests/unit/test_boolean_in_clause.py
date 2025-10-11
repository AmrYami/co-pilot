from pathlib import Path
import sys
import types

sys.path.append(str(Path(__file__).resolve().parents[2]))

sqlalchemy_stub = types.ModuleType("sqlalchemy")
sqlalchemy_stub.text = lambda sql, **_: sql
sys.modules.setdefault("sqlalchemy", sqlalchemy_stub)

sqlglot_stub = types.ModuleType("sqlglot")


class _SqlglotDummy:
    args: dict = {}

    def find(self, *_args, **_kwargs):
        return None


sqlglot_stub.parse_one = lambda sql, read=None: _SqlglotDummy()
exp_stub = types.ModuleType("sqlglot.exp")
exp_stub.Select = type("Select", (), {})
sqlglot_stub.exp = exp_stub
sys.modules.setdefault("sqlglot", sqlglot_stub)
sys.modules.setdefault("sqlglot.exp", exp_stub)

from apps.dw.contracts.builder import build_boolean_where_from_question


def test_eq_or_values_fold_to_in():
    question = "ENTITY = DSFH or Farabi"
    where_sql, binds = build_boolean_where_from_question(
        question,
        fts_columns=[],
        allowed_columns={"ENTITY"},
    )

    assert where_sql, "Expected SQL clause for OR equality values"
    assert "UPPER(TRIM(ENTITY)) IN (UPPER(TRIM(:eq_bg_0)), UPPER(TRIM(:eq_bg_1)))" in where_sql
    assert "UPPER(TRIM(ENTITY)) =" not in where_sql
    assert binds == {"eq_bg_0": "DSFH", "eq_bg_1": "FARABI"}
