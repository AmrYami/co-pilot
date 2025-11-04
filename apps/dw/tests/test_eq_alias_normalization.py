from apps.dw.app import _apply_online_rate_hints, _expand_eq_aliases_with_map


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
