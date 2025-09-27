from __future__ import annotations
import re
from flask import Blueprint

from ..contracts.contract_common import OVERLAP_PRED
from ..contracts.contract_planner import plan_contract_query
from .golden_dw_contracts_yaml_loader import load_golden


def _like(sql: str, pattern: str, overlap_text: str) -> bool:
    pat = pattern.replace("%(OVERLAP)s", re.escape(overlap_text))
    parts = [re.escape(p.strip()) for p in pat.split("%") if p.strip()]
    regex = ".*".join(parts)
    return re.search(regex, sql, re.S | re.I) is not None


tests_bp = Blueprint("dw_tests", __name__)


@tests_bp.get("/dw/tests/run_golden")
def run_golden():
    golden = load_golden()
    results = []
    for case in golden.get("tests", []):
        question = case.get("question", "")
        sql, binds, meta, explain = plan_contract_query(
            question,
            explicit_dates=None,
            top_n=None,
            full_text_search=False,
            fts_columns=[],
            fts_tokens=[],
        )
        ok = _like(sql, case.get("expect_like", ""), OVERLAP_PRED)
        results.append({"name": case.get("name"), "ok": ok, "sql": sql})
    return {"ok": True, "results": results}
