from datetime import datetime
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from apps.dw.nlu_normalizer import DEFAULT_TZ, NET_VALUE_EXPR, normalize


def test_normalize_count_last_month():
    now = datetime(2024, 2, 15, tzinfo=DEFAULT_TZ)
    intent = normalize("count contracts last month", now=now)
    assert intent.agg == "count"
    assert intent.wants_all_columns is False
    assert intent.has_time_window is True
    assert intent.explicit_dates
    assert intent.explicit_dates.start == "2024-01-01"
    assert intent.explicit_dates.end == "2024-01-31"
    assert intent.date_column == "REQUEST_DATE"


def test_normalize_top_stakeholders_last_month():
    now = datetime(2023, 5, 20, tzinfo=DEFAULT_TZ)
    intent = normalize("top five stakeholders by contract value last month", now=now)
    assert intent.top_n == 5
    assert intent.user_requested_top_n is True
    assert intent.group_by == "CONTRACT_STAKEHOLDER_1"
    assert intent.sort_by == NET_VALUE_EXPR
    assert intent.measure_sql == NET_VALUE_EXPR
    assert intent.has_time_window is True


def test_normalize_expiring_next_two_weeks():
    now = datetime(2023, 1, 10, tzinfo=DEFAULT_TZ)
    intent = normalize("contracts expiring next 2 weeks", now=now)
    assert intent.date_column == "END_DATE"
    assert intent.has_time_window is True
    assert intent.explicit_dates
    assert intent.explicit_dates.start == "2023-01-10"
    assert intent.explicit_dates.end == "2023-01-24"


def test_normalize_arabic_top_entities_this_year():
    now = datetime(2023, 7, 1, tzinfo=DEFAULT_TZ)
    intent = normalize("أفضل 3 جهات حسب قيمة العقد هذا العام", now=now)
    assert intent.top_n == 3
    assert intent.group_by == "ENTITY_NO"
    assert intent.measure_sql == NET_VALUE_EXPR
    assert intent.has_time_window is True
    assert intent.explicit_dates
    assert intent.explicit_dates.start == "2023-01-01"
    assert intent.explicit_dates.end == "2023-12-31"
