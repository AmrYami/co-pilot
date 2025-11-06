import json

from sqlalchemy import create_engine, text

from apps.dw.app import _apply_online_rate_hints, _expand_eq_aliases_with_map, _drop_like_when_in
from apps.dw.learning import load_rules_for_question


def _collect_eq_bind_values(binds):
    return {str(value) for key, value in binds.items() if key.startswith("ol_eq")}


def test_alias_eq_filters_before_expansion_retains_partial_value():
    sql = 'SELECT * FROM "Contract"'
    _, binds, _ = _apply_online_rate_hints(
        sql,
        {},
        {"eq_filters": [{"col": "DEPARTMENT_1", "val": "FACILITY"}]},
    )

    assert "FACILITY" in _collect_eq_bind_values(binds)


def test_alias_expansion_prefers_canonical_department_value():
    intent = {
        "eq_filters": [
            ["DEPARTMENT", ["FACILITY", "FACILITY AND SITE SERVICES (FARABI)"]],
            {"col": "DEPARTMENT_1", "val": "FACILITY"},
        ],
        "or_groups": [],
    }
    alias_map = {
        "DEPARTMENT": [
            "DEPARTMENT_1",
            "DEPARTMENT_2",
            "DEPARTMENT_3",
            "DEPARTMENT_4",
            "DEPARTMENT_5",
            "DEPARTMENT_6",
            "DEPARTMENT_7",
            "DEPARTMENT_8",
            "OWNER_DEPARTMENT",
        ],
        "DEPARTMENTS": [
            "DEPARTMENT_1",
            "DEPARTMENT_2",
            "DEPARTMENT_3",
            "DEPARTMENT_4",
            "DEPARTMENT_5",
            "DEPARTMENT_6",
            "DEPARTMENT_7",
            "DEPARTMENT_8",
            "OWNER_DEPARTMENT",
        ],
    }

    _expand_eq_aliases_with_map(intent, alias_map)
    _, binds, _ = _apply_online_rate_hints('SELECT * FROM "Contract"', {}, intent)

    eq_values = _collect_eq_bind_values(binds)
    assert "FACILITY AND SITE SERVICES (FARABI)" in eq_values
    assert "FACILITY" not in eq_values


def test_drop_like_when_in_respects_enabled_flag():
    sql = (
        'SELECT * FROM "Contract"\n'
        "WHERE (UPPER(TRIM(DEPARTMENT_1)) LIKE UPPER(TRIM(:eq_bg_0)))"
    )
    binds = {"eq_bg_0": "%SUPPORT SERVICES%"}
    eq_alias_targets = {"DEPARTMENT": ["DEPARTMENT_1", "DEPARTMENT_2"]}

    guarded_sql, guarded_binds = _drop_like_when_in(
        sql,
        dict(binds),
        eq_alias_targets,
        enabled=True,
    )
    assert "LIKE" not in guarded_sql
    assert "eq_bg_0" not in guarded_binds

    original_sql, original_binds = _drop_like_when_in(
        sql,
        dict(binds),
        eq_alias_targets,
        enabled=False,
    )
    assert "LIKE" in original_sql
    assert "eq_bg_0" in original_binds


def test_eq_shape_guard_prefers_question_values(monkeypatch):
    engine = create_engine("sqlite:///:memory:", future=True)
    with engine.begin() as cx:
        cx.execute(
            text(
                """
                CREATE TABLE dw_rules (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    question_norm TEXT,
                    rule_kind TEXT,
                    rule_payload TEXT,
                    enabled BOOLEAN DEFAULT 1,
                    rule_signature TEXT,
                    intent_sig TEXT,
                    intent_sha TEXT
                )
                """
            )
        )
        cx.execute(
            text(
                """
                INSERT INTO dw_rules (question_norm, rule_kind, rule_payload, enabled)
                VALUES ('', 'eq_shape', :payload, 1)
                """
            ),
            {
                "payload": json.dumps(
                    {
                        "items": [
                            {
                                "logical": "DEPARTMENT",
                                "columns": [
                                    "DEPARTMENT_1",
                                    "DEPARTMENT_2",
                                    "DEPARTMENT_3",
                                ],
                            }
                        ]
                    }
                )
            },
        )
        cx.execute(
            text(
                """
                INSERT INTO dw_rules (question_norm, rule_kind, rule_payload, enabled)
                VALUES ('', 'eq', :payload, 1)
                """
            ),
            {
                "payload": json.dumps(
                    {
                        "eq_filters": [
                            ["DEPARTMENT_1", ["SUPPORT SERVICES"]],
                            ["DEPARTMENT_2", ["SUPPORT SERVICES"]],
                        ]
                    }
                )
            },
        )

    merged = load_rules_for_question(
        engine,
        "list all contracts where department = support services",
        intent={"eq_filters": [["DEPARTMENT", ["Support Services Updated"]]]},
    )

    assert merged.get("eq_filters") == [["DEPARTMENT", ["Support Services Updated"]]]
