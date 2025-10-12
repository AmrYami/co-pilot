"""Tests for case-insensitive REQUEST_TYPE handling in /dw/rate."""

import pytest

pytest.importorskip("flask")

from apps.dw.tests.test_rate_basic import client  # re-use fixture


def test_rate_request_type_case_insensitive(client):
    payload = {"inquiry_id": 30, "rating": 1, "comment": "eq: REQUEST_TYPE = Renewal;"}
    resp = client.post("/dw/rate", json=payload)
    assert resp.status_code == 200

    data = resp.get_json()
    sql = data["debug"]["final_sql"]["sql"]

    assert "UPPER(TRIM(REQUEST_TYPE))" in sql
    assert "LIKE UPPER(:eq_" in sql or "LIKE UPPER(:rt_pre_" in sql
    assert "ORDER BY REQUEST_DATE DESC" in sql
