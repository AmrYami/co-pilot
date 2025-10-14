"""Unit tests for the lightweight /dw/rate core parser and builder."""

from __future__ import annotations

from datetime import date
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parents[1]))

from apps.dw.rate import RateIntent, build_sql, parse_rate_comment


def _sample_settings() -> dict:
    return {
        "DW_CONTRACT_TABLE": "Contract",
        "DW_FTS_COLUMNS": {"Contract": ["SUBJECT", "NOTES"]},
        "DW_EQ_ALIAS_COLUMNS": {"DEPARTMENT": ["DEPT_A", "DEPT_B"]},
        "DW_ENUM_SYNONYMS": {
            "Contract.REQUEST_TYPE": {
                "renewal": {
                    "equals": ["Renewal", "contract renewal"],
                    "prefix": ["ren"],
                    "contains": ["extension"],
                }
            }
        },
        "DW_DATE_COLUMN": "REQUEST_DATE",
    }


def test_parse_rate_comment_basic_fields():
    settings = _sample_settings()
    intent = parse_rate_comment(
        "eq: REQUEST_TYPE = Renewal; "
        "contains: NOTES = urgent; "
        "not_contains: notes = draft; "
        "empty_any: notes, comments; "
        "order_by: END_DATE asc; "
        "limit: 5; offset: 10; last month",
        settings,
    )

    assert isinstance(intent, RateIntent)
    assert intent.eq_filters == [("REQUEST_TYPE", ["Renewal"])]
    assert intent.contains == [("NOTES", ["urgent"])]
    assert intent.not_contains == [("notes", ["draft"])]
    assert intent.empty_any == [["notes", "comments"]]
    assert intent.order_by == [("END_DATE", "asc")]
    assert intent.limit == 5 and intent.offset == 10
    assert intent.when_kind == "active"
    assert isinstance(intent.date_start, date) and isinstance(intent.date_end, date)


def test_build_sql_with_synonyms_and_pagination():
    settings = _sample_settings()
    intent = parse_rate_comment(
        "eq: REQUEST_TYPE = Renewal; "
        "eq: department = Clinic; "
        "fts: urgent; "
        "order_by: END_DATE asc; "
        "limit: 5; offset: 2; last month",
        settings,
    )
    sql, binds = build_sql(intent, settings)

    assert "START_DATE <= :date_end" in sql and "END_DATE >= :date_start" in sql
    assert "ORDER BY END_DATE ASC" in sql
    assert "OFFSET" in sql and "FETCH NEXT" in sql
    assert any(key.startswith("eq") for key in binds), "EQ binds missing"
    assert any(key.startswith("pre") for key in binds), "prefix bind missing"
    assert any(key.startswith("contains") for key in binds), "contains bind missing"
    assert any(key.startswith("fts") for key in binds), "fts bind missing"
    assert any(key.startswith("offset") for key in binds)
    assert any(key.startswith("limit") for key in binds)

