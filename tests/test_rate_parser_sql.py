import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from apps.dw.rate_parser import parse_rate_comment
from apps.dw.rate_sql import build_where


def _sql(intent):
    where, binds = build_where(intent)
    return where, binds


def test_fts_or():
    intent = parse_rate_comment("fts: it or home care")
    intent["_fts_columns"] = ["CONTRACT_SUBJECT"]
    where, binds = _sql(intent)
    assert "LIKE" in where
    assert "fts_0" in binds and "fts_1" in binds


def test_eq_synonyms_request_type():
    intent = parse_rate_comment("eq: REQUEST_TYPE = renewal")
    where, binds = _sql(intent)
    assert "REQUEST_TYPE" in where
    assert any(k.startswith("eq_") for k in binds.keys())


def test_not_equals_and_not_contains_and_empty():
    intent = parse_rate_comment(
        "neq: ENTITY = DSFH; not_contains: CONTRACT_SUBJECT = cloud; empty: REPRESENTATIVE_EMAIL"
    )
    where, binds = _sql(intent)
    assert " <> " in where and " NOT LIKE " in where and "NVL(REPRESENTATIVE_EMAIL" in where


def test_empty_any_all():
    intent = parse_rate_comment(
        "empty_any: DEPARTMENT_1, DEPARTMENT_2; empty_all: STAKEHOLDER_1, STAKEHOLDER_2"
    )
    where, _ = _sql(intent)
    assert "DEPARTMENT_1" in where and "DEPARTMENT_2" in where
    assert "STAKEHOLDER_1" in where and "STAKEHOLDER_2" in where
