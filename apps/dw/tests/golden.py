from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any, Dict, List

import yaml

from apps.dw.tables.contracts import plan_sql


def run_golden(file_path: str | Path) -> List[Dict[str, Any]]:
    fp = Path(file_path)
    data = yaml.safe_load(fp.read_text(encoding="utf-8"))
    results: List[Dict[str, Any]] = []
    for case in data.get("tests", []):
        question = case["question"]
        sql, binds, meta = plan_sql(question, today=date.today())
        ok = True
        reasons: List[str] = []
        for must in case.get("must_contain", []):
            if must not in sql:
                ok = False
                reasons.append(f"missing: {must}")
        for forbid in case.get("forbid", []):
            if forbid in sql:
                ok = False
                reasons.append(f"forbidden present: {forbid}")
        results.append(
            {
                "id": case.get("id"),
                "question": question,
                "ok": ok,
                "reasons": reasons,
                "sql": sql,
                "binds": binds,
                "meta": meta,
            }
        )
    return results
