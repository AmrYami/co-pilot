import json
import pathlib
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from apps.dw.app import derive_sql_for_test  # noqa: E402
from apps.dw.learning_store import (  # noqa: E402
    reset_family_map_cache,
    reset_signature_knobs_cache,
)


@pytest.fixture(autouse=True)
def _reset_settings(monkeypatch):
    monkeypatch.delenv("DW_EQ_ALIAS_COLUMNS", raising=False)
    reset_signature_knobs_cache()
    reset_family_map_cache()
    yield
    monkeypatch.delenv("DW_EQ_ALIAS_COLUMNS", raising=False)
    reset_signature_knobs_cache()
    reset_family_map_cache()


def test_alias_same_sql_diff_binds(monkeypatch):
    alias_map = json.dumps(
        {
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
            ]
        }
    )
    monkeypatch.setenv("DW_EQ_ALIAS_COLUMNS", alias_map)
    monkeypatch.setattr("apps.dw.app._get_pipeline", lambda: None)
    reset_signature_knobs_cache()
    reset_family_map_cache()

    sql_one, binds_one = derive_sql_for_test(
        "list all contracts where departments = SUPPORT SERVICES"
    )
    sql_two, binds_two = derive_sql_for_test(
        "list all contracts where departments = SUPPORT SERVICES HQ"
    )

    assert sql_one == sql_two
    assert binds_one != binds_two
