import pathlib
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from apps.dw.search import build_fulltext_where, extract_search_tokens, inject_fulltext_where


def test_extract_search_tokens_handles_phrases(monkeypatch):
    monkeypatch.setenv("DW_FTS_MIN_TOKEN_LEN", "3")
    question = 'Show "Acme Holdings" contracts with VAT and the status'
    tokens = extract_search_tokens(question)
    assert "acme holdings" in tokens
    assert "vat" in tokens
    assert "contracts" not in tokens


def test_inject_fulltext_where_with_existing_where():
    sql = 'SELECT * FROM "Contract"\nWHERE OWNER_DEPARTMENT = :dep\nORDER BY 1'
    predicate = '(LOWER("CONTRACT_OWNER") LIKE :kw1)'
    updated = inject_fulltext_where(sql, predicate)
    assert 'AND (LOWER("CONTRACT_OWNER") LIKE :kw1)' in updated
    assert updated.endswith("ORDER BY 1")


def test_build_fulltext_where_like_builder():
    groups = [["acme"], ["services"]]
    predicate, binds, error = build_fulltext_where(
        ["NAME", "OWNER"], groups, engine="like"
    )
    assert error is None
    assert predicate.count("LIKE") == 4
    assert binds == {"fts_0": "%acme%", "fts_1": "%services%"}
