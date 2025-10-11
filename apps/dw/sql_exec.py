from typing import Any, Dict

from .logs import jlog, scrub_binds


def execute_sql(engine: Any, sql_text: str, binds: Dict[str, Any], ctx: Dict[str, Any]):
    jlog(
        "rate.sql.execute",
        trace_id=ctx.get("trace_id"),
        sql_preview=(sql_text or "")[:300],
        binds=scrub_binds(binds),
    )
    rows = engine.execute(sql_text, binds)
    try:
        count = len(rows)
    except Exception:
        count = getattr(rows, "rowcount", None)
    jlog("rate.exec", trace_id=ctx.get("trace_id"), rowcount=count)
    return rows
