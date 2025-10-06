"""Tests for the /dw/rate structured comment parser."""

from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parents[1]))

from apps.dw.rate_comment import parse_rate_comment


def test_parse_rate_comment_with_fts_and_order():
    comment = "fts: it | home care; order_by: request_date asc;"
    result = parse_rate_comment(comment)

    assert result["fts_tokens"] == ["it", "home care"]
    assert result["fts_operator"] == "OR"
    assert result["sort_by"] == "REQUEST_DATE"
    assert result["sort_desc"] is False


def test_parse_rate_comment_with_and_operator_and_flags():
    comment = "eq: entity = DSFH (ci, trim); fts: acute & oncology"
    result = parse_rate_comment(comment)

    assert result["fts_tokens"] == ["acute", "oncology"]
    assert result["fts_operator"] == "AND"
    assert result["eq_filters"] == [
        {"col": "ENTITY", "val": "DSFH", "ci": True, "trim": True}
    ]


def test_parse_rate_comment_multi_eq_and_group_by():
    comment = "eq: entity = DSFH; request_type = 'Renewal'; group_by: owner_department"
    result = parse_rate_comment(comment)

    assert result["group_by"] == "OWNER_DEPARTMENT"
    assert result["eq_filters"] == [
        {"col": "ENTITY", "val": "DSFH", "ci": False, "trim": False},
        {"col": "REQUEST_TYPE", "val": "Renewal", "ci": False, "trim": False},
    ]


def test_parse_rate_comment_preserves_parentheses_in_value():
    comment = "eq: notes = 'ACME (Holdings)'"
    result = parse_rate_comment(comment)

    assert result["eq_filters"] == [
        {"col": "NOTES", "val": "ACME (Holdings)", "ci": False, "trim": False}
    ]


def test_parse_rate_comment_gross_boolean():
    comment = "gross: true"
    result = parse_rate_comment(comment)

    assert result["gross"] is True
