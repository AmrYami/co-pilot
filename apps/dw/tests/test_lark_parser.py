import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from apps.dw.app import IntentPipelineConfig, _build_light_intent_via_lark
from apps.dw.intent_parser import DwQuestionParser


def _config() -> IntentPipelineConfig:
    return IntentPipelineConfig(
        pipeline_version="pytest",
        parser="lark",
        use_lark=True,
        eq_value_policy="question_only",
        tail_trim=False,
        signature_mode="signature_first",
    )


def test_lark_builds_aggregation_intent_with_group_by_and_order() -> None:
    config = _config()
    intent = _build_light_intent_via_lark(
        "For ENTITY_NO = 'E-123', total and count by CONTRACT_STATUS",
        allowed_cols=["ENTITY_NO", "CONTRACT_STATUS", "CONTRACT_VALUE_NET_OF_VAT"],
        alias_map={},
        config=config,
    )

    assert intent["eq_filters"] == [["ENTITY_NO", ["E-123"]]]
    assert intent["aggregations"] == [
        {"func": "SUM", "column": "CONTRACT_VALUE_NET_OF_VAT", "alias": "TOTAL_AMOUNT", "distinct": False},
        {"func": "COUNT", "column": "*", "alias": "TOTAL_COUNT", "distinct": False},
    ]
    assert intent["group_by"] == ["CONTRACT_STATUS"]
    assert intent["order"] == {"col": "CONTRACT_STATUS", "desc": False}
    assert intent["_meta"]["segments"]["aggregations"] == 2


def test_lark_handles_aggregation_phrase_without_comma() -> None:
    config = _config()
    intent = _build_light_intent_via_lark(
        "For entity_no = 'E-123' total and count by contract_status",
        allowed_cols=["ENTITY_NO", "CONTRACT_STATUS", "CONTRACT_VALUE_NET_OF_VAT"],
        alias_map={},
        config=config,
    )

    assert intent["eq_filters"] == [["ENTITY_NO", ["E-123"]]]
    assert {agg["alias"] for agg in intent["aggregations"]} == {"TOTAL_AMOUNT", "TOTAL_COUNT"}
    assert intent["group_by"] == ["CONTRACT_STATUS"]


def test_lark_parser_alias_normalization() -> None:
    parser = DwQuestionParser()
    parsed = parser.parse("departments = SUPPORT SERVICES")

    assert parsed.eq_filters == [["DEPARTMENTS", ["SUPPORT SERVICES"]]]
    assert not parsed.aggregations


def test_lark_parser_accepts_quoted_alias() -> None:
    parser = DwQuestionParser()
    parsed = parser.parse('For "ENTITY_NO" = "E-123", total and count by CONTRACT_STATUS')

    assert parsed.eq_filters == [["ENTITY_NO", ["E-123"]]]
    assert parsed.aggregations


def test_lark_parser_allows_and_inside_value() -> None:
    config = _config()
    intent = _build_light_intent_via_lark(
        "owner = FACILITY AND SITE SERVICES (FARABI)",
        allowed_cols=["OWNER"],
        alias_map={"OWNER": ["CONTRACT_OWNER", "OWNER_DEPARTMENT"]},
        config=config,
    )

    eq_filters = intent.get("eq_filters") or []
    assert eq_filters == [["OWNER", ["FACILITY AND SITE SERVICES (FARABI)"]]]
