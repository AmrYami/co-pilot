import pathlib
import sys
from typing import Any, Dict, List

ROOT = pathlib.Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from apps.dw.app import _apply_online_rate_hints, _expand_eq_aliases_with_map  # noqa: E402


def test_expand_eq_aliases_collapses_variants():
    intent = {
        "eq_filters": [
            ["DEPARTMENTS", ["SUPPORT SERVICE"]],
            ["DEPARTMENTS", ["SUPPORT SERVICES"]],
        ]
    }
    alias_map = {"DEPARTMENTS": ["DEPARTMENT_1", "OWNER_DEPARTMENT"]}
    _expand_eq_aliases_with_map(intent, alias_map)

    assert intent.get("eq_filters") == []
    groups = intent.get("or_groups") or []
    assert groups, "alias expansion should create or_groups"
    first_group = groups[0]
    # All targets should share the canonical plural form.
    for entry in first_group:
        assert entry["values"] == ["SUPPORT SERVICES"]


def test_expand_eq_aliases_removes_partial_duplicates():
    intent = {
        "eq_filters": [
            ["OWNER", ["FACILITY", "FACILITY AND SITE SERVICES (FARABI)"]],
        ]
    }
    alias_map = {"OWNER": ["CONTRACT_OWNER", "OWNER_DEPARTMENT"]}
    _expand_eq_aliases_with_map(intent, alias_map)

    assert intent.get("eq_filters") == []
    groups = intent.get("or_groups") or []
    assert groups and len(groups) == 1
    first_group = groups[0]
    values_sets = {tuple(term.get("values", [])) for term in first_group}
    assert values_sets == {("FACILITY AND SITE SERVICES (FARABI)",)}


def test_expand_eq_aliases_replaces_existing_group_values():
    intent = {
        "eq_filters": [
            ["OWNER", ["FACILITY AND SITE SERVICES (FARABI)"]],
        ],
        "or_groups": [
            [
                {"col": "CONTRACT_OWNER", "values": ["FACILITY"], "op": "eq", "ci": True, "trim": True},
                {"col": "OWNER_DEPARTMENT", "values": ["FACILITY"], "op": "eq", "ci": True, "trim": True},
            ]
        ],
    }
    alias_map = {"OWNER": ["CONTRACT_OWNER", "OWNER_DEPARTMENT"]}
    _expand_eq_aliases_with_map(intent, alias_map)

    assert intent.get("eq_filters") == []
    groups = intent.get("or_groups") or []
    assert groups and len(groups) == 1
    first_group = groups[0]
    values_sets = {tuple(term.get("values", [])) for term in first_group}
    assert values_sets == {("FACILITY AND SITE SERVICES (FARABI)",)}


def test_order_hint_guard_keeps_aggregate_order():
    base_sql = (
        'SELECT CONTRACT_STATUS AS GROUP_KEY,\n'
        '       SUM(NVL(CONTRACT_VALUE_NET_OF_VAT,0)) AS MEASURE,\n'
        '       COUNT(*) AS CNT\n'
        'FROM "Contract"\n'
        "WHERE ENTITY_NO = :entity_no\n"
        "GROUP BY CONTRACT_STATUS\n"
        "ORDER BY MEASURE DESC"
    )
    binds = {"entity_no": "e-123"}
    intent_patch = {
        "sort_by": "REQUEST_DATE",
        "sort_desc": True,
        "eq_filters": [
            {"col": "ENTITY_NO", "val": "E-123", "op": "eq", "ci": True, "trim": True}
        ],
    }

    new_sql, new_binds, meta = _apply_online_rate_hints(base_sql, dict(binds), intent_patch)

    assert "ORDER BY MEASURE DESC" in new_sql
    assert "REQUEST_DATE" not in new_sql
    assert meta.get("order_by") is None
    assert new_binds["entity_no"] == "e-123"
    assert any(name.startswith("ol_eq_") for name in new_binds if isinstance(name, str))


def test_light_intent_entity_no_paraphrase():
    from apps.dw.app import _build_light_intent_from_question

    intent = _build_light_intent_from_question(
        "total and count by CONTRACT_STATUS for entity no E-123", ["ENTITY_NO"]
    )
    eq_filters = intent.get("eq_filters") or []
    assert any(col == "ENTITY_NO" and "E-123" in values for col, values in eq_filters if isinstance(col, str))


def test_signature_loader_merges_aggregation_rules(monkeypatch):
    from apps.dw.learning import load_rules_for_question

    question = 'For "ENTITY_NO" = "E-123", total and count by CONTRACT_STATUS'
    intent = {
        "eq_filters": [["ENTITY_NO", ["E-123"]]],
        "aggregations": [
            {
                "func": "SUM",
                "column": "CONTRACT_VALUE_NET_OF_VAT",
                "alias": "TOTAL_AMOUNT",
                "distinct": False,
            },
            {
                "func": "COUNT",
                "column": "*",
                "alias": "TOTAL_COUNT",
                "distinct": False,
            },
        ],
        "group_by": ["CONTRACT_STATUS"],
        "sort_by": "CONTRACT_STATUS",
        "sort_desc": False,
    }

    rows = [
        {
            "rule_kind": "agg",
            "rule_payload": {
                "aggregations": [
                    {
                        "func": "SUM",
                        "column": "CONTRACT_VALUE_NET_OF_VAT",
                        "alias": "TOTAL_AMOUNT",
                        "distinct": False,
                    },
                    {
                        "func": "COUNT",
                        "column": "*",
                        "alias": "TOTAL_COUNT",
                        "distinct": False,
                    },
                ]
            },
        },
        {
            "rule_kind": "group_by",
            "rule_payload": {"group_by": ["CONTRACT_STATUS"], "gross": False},
        },
        {
            "rule_kind": "order_by",
            "rule_payload": {"sort_by": "CONTRACT_STATUS", "sort_desc": False},
        },
        {
            "rule_kind": "eq",
            "rule_payload": {"eq_filters": [["ENTITY_NO", ["E-123"]]]},
        },
    ]

    class _Result:
        def __init__(self, payload):
            self._payload = payload

        def mappings(self):
            return self

        def all(self):
            return list(self._payload)

    class _Conn:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql, binds):
            return _Result(rows)

    class _Engine:
        def connect(self):
            return _Conn()

    monkeypatch.setattr("apps.dw.learning._ensure_tables", lambda engine: None)
    monkeypatch.setattr("apps.dw.learning.eq_alias_columns", lambda: {})

    merged = load_rules_for_question(_Engine(), question, intent=intent)

    assert merged.get("aggregations")
    aliases = {agg.get("alias") for agg in merged["aggregations"]}
    assert {"TOTAL_AMOUNT", "TOTAL_COUNT"} <= aliases
    assert merged.get("group_by") == ["CONTRACT_STATUS"]
    assert merged.get("sort_by") == "CONTRACT_STATUS"


def test_rate_comment_numeric_filters():
    from apps.dw.rate_grammar import parse_rate_comment

    comment = (
        "eq: CONTRACT_STATUS = Active; "
        "ge: CONTRACT_VALUE_NET_OF_VAT = 200000; "
        "order_by: CONTRACT_VALUE_NET_OF_VAT desc;"
    )
    hints = parse_rate_comment(comment)

    assert any(f.get("col") == "CONTRACT_STATUS" for f in hints.get("eq_filters", []))
    numeric = hints.get("numeric_filters") or []
    assert numeric and numeric[0]["op"] == "gte"

    base_sql = 'SELECT * FROM "Contract"'
    sql, binds, _ = _apply_online_rate_hints(base_sql, {}, hints)
    assert "CONTRACT_VALUE_NET_OF_VAT >=" in sql
    assert any(v == 200000 or v == "200000" for v in binds.values())


def test_rate_comment_v2_numeric_filters():
    from apps.dw.rate_comment import parse_rate_comment as parse_rate_comment_v2
    from apps.dw.sql.builder import QueryBuilder

    comment = (
        "eq: CONTRACT_STATUS = Active; "
        "ge: CONTRACT_VALUE_NET_OF_VAT = 200000; "
        "order_by: CONTRACT_VALUE_NET_OF_VAT desc;"
    )
    hints = parse_rate_comment_v2(comment)

    numeric = hints.get("numeric_filters") or []
    assert numeric and numeric[0]["col"] == "CONTRACT_VALUE_NET_OF_VAT"
    qb = QueryBuilder(table="Contract", date_col="REQUEST_DATE")
    qb.apply_numeric_filters(numeric)
    sql, binds = qb.compile()
    assert "CONTRACT_VALUE_NET_OF_VAT >=" in sql
    assert any(name.startswith("num_") for name in binds)


def test_stakeholder_has_trims_following_clause_contract_planner():
    from apps.dw.contracts.contract_planner import _apply_stakeholder_has

    where_parts: List[str] = []
    binds: Dict[str, Any] = {}
    applied, terms = _apply_stakeholder_has(
        "stakeholder has Alice Smith or Bob Jones and department = SUPPORT SERVICES",
        where_parts,
        binds,
    )

    assert applied
    assert terms == ["Alice Smith", "Bob Jones"]
    assert where_parts and "department" not in where_parts[0].lower()
    assert all("DEPARTMENT" not in value.upper() for value in binds.values() if isinstance(value, str))


def test_stakeholder_has_trims_following_clause_table_parser():
    from apps.dw.tables.contracts import _maybe_apply_stakeholder_filters
    from apps.dw.tables.contracts import Intent  # type: ignore[attr-defined]

    intent = Intent(question="")  # type: ignore[call-arg]
    intent.raw_question = "stakeholder has Alice Smith or Bob Jones and department = SUPPORT SERVICES"
    where_parts: List[str] = []
    binds: Dict[str, Any] = {}
    _maybe_apply_stakeholder_filters(intent, where_parts, binds)  # type: ignore[attr-defined]

    assert where_parts and "department" not in where_parts[0].lower()
    assert all("DEPARTMENT" not in value.upper() for value in binds.values() if isinstance(value, str))
