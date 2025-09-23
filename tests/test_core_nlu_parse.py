from datetime import date

from core.nlu.parse import parse_intent
from core.nlu.time import resolve_window


def test_parse_intent_top_stakeholders():
    question = "Top five stakeholders by gross value"
    intent = parse_intent(question, default_date_col="REQUEST_DATE", select_all_default=True)
    assert intent.group_by == "CONTRACT_STAKEHOLDER_1"
    assert intent.top_n == 5
    assert intent.user_requested_top_n is True
    assert intent.measure_sql and "VAT" in intent.measure_sql
    assert intent.sort_by == intent.measure_sql
    assert intent.wants_all_columns is False
    assert intent.notes.get("q") == question


def test_resolve_window_last_quarter():
    window = resolve_window("last quarter", now=date(2024, 5, 10))
    assert window
    assert window.start == "2024-01-01"
    assert window.end == "2024-03-31"


def test_resolve_window_next_10_days():
    window = resolve_window("next 10 days", now=date(2024, 2, 15))
    assert window
    assert window.start == "2024-02-15"
    assert window.end == "2024-02-25"
