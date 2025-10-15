import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from apps.dw.rate_pipeline import build_select_all


def norm(sql: str) -> str:
    return re.sub(r"\s+", " ", sql.strip())


def test_build_select_all_no_order_by():
    sql = build_select_all("Contract")
    assert norm(sql) == 'SELECT * FROM "Contract"'


def test_build_select_all_with_order_by_desc():
    sql = build_select_all("Contract", "REQUEST_DATE", True)
    assert "ORDER BY REQUEST_DATE DESC" in norm(sql)


def test_build_select_all_with_order_by_asc():
    sql = build_select_all("Contract", "REQUEST_DATE", False)
    assert "ORDER BY REQUEST_DATE ASC" in norm(sql)
