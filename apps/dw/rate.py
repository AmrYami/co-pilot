from __future__ import annotations

from datetime import datetime
import re
from flask import Blueprint, request, jsonify

from .settings import get_setting
from .sql_builder import build_measure_sql, quote_ident, strip_double_order_by
from .learn_store import LearningStore, ExampleRecord, PatchRecord
from .utils import safe_upper

rate_bp = Blueprint("rate", __name__)

# ---------------------------
# Rate Comment Parsing
# ---------------------------
_WS = r"[ \t]*"
_IDENT = r"[A-Za-z0-9_ ]+"


def _parse_csv(s: str) -> list[str]:
    parts = re.split(r"[,\|]", s or "")
    return [p.strip() for p in parts if p and p.strip()]


def _norm_bool(s: str) -> bool | None:
    if s is None:
        return None
    s2 = s.strip().lower()
    if s2 in ("true", "yes", "y", "1"):
        return True
    if s2 in ("false", "no", "n", "0"):
        return False
    return None


def parse_rate_comment(raw: str) -> dict:
    intent: dict = {
        "fts_tokens": [],
        "fts_operator": "OR",
        "full_text_search": False,
        "eq_filters": [],
        "group_by": None,
        "sort_by": None,
        "sort_desc": None,
        "gross": None,
    }
    if not raw:
        return intent

    text = raw.strip()

    m = re.search(r"\bfts\s*:\s*(.+?)(?:$|\n|;)", text, flags=re.IGNORECASE)
    if m:
        toks = m.group(1)
        intent["fts_tokens"] = _parse_csv(toks)
        intent["full_text_search"] = True

    m = re.search(r"\bgroup_by\s*:\s*(.+?)(?:$|\n|;)", text, flags=re.IGNORECASE)
    if m:
        cols = _parse_csv(m.group(1))
        if cols:
            intent["group_by"] = cols[0]

    m = re.search(r"\bgross\s*:\s*(.+?)(?:$|\n|;)", text, flags=re.IGNORECASE)
    if m:
        intent["gross"] = _norm_bool(m.group(1))

    m = re.search(r"\border_by\s*:\s*([A-Za-z0-9_ ]+)(?:\s+(asc|desc))?", text, flags=re.IGNORECASE)
    if m:
        intent["sort_by"] = m.group(1).strip()
        if m.group(2):
            intent["sort_desc"] = m.group(2).lower() == "desc"

    for eq_m in re.finditer(r"\beq\s*:\s*(.+?)(?:$|\n)", text, flags=re.IGNORECASE):
        eq_body = eq_m.group(1)
        for clause in re.split(r";", eq_body):
            c = clause.strip()
            if not c:
                continue
            flags = {"ci": False, "trim": False}
            flags_m = re.search(r"\(([^)]*)\)\s*$", c)
            if flags_m:
                flag_text = flags_m.group(1).lower()
                flags["ci"] = "ci" in flag_text
                flags["trim"] = "trim" in flag_text
                c = c[: flags_m.start()].strip()
            m2 = re.match(rf"({_IDENT}){_WS}={_WS}(.+)$", c)
            if not m2:
                continue
            col = m2.group(1).strip()
            val = m2.group(2).strip()
            if (val.startswith("'") and val.endswith("'")) or (
                val.startswith('"') and val.endswith('"')
            ):
                val = val[1:-1]
            intent["eq_filters"].append(
                {
                    "col": col,
                    "val": val,
                    "ci": bool(flags["ci"]),
                    "trim": bool(flags["trim"]),
                    "op": "eq",
                }
            )
    return intent


def _select_rate_fts_columns() -> list[str]:
    cfg = get_setting("DW_FTS_COLUMNS", scope="namespace") or {}
    cols = cfg.get("Contract") or cfg.get("*") or []
    seen = set()
    out = []
    for c in cols:
        u = c.upper()
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


def _resolve_group_by_col(raw_col: str) -> str | None:
    if not raw_col:
        return None
    allowed = get_setting("DW_EXPLICIT_FILTER_COLUMNS", scope="namespace") or []
    rc = raw_col.strip().upper()
    for c in allowed:
        if rc == c.upper():
            return c
    rc2 = rc.replace(" ", "")
    for c in allowed:
        if rc2 == c.upper().replace(" ", ""):
            return c
    return None


def _build_eq_where(eq_filters: list[dict], binds: dict) -> list[str]:
    where = []
    for i, f in enumerate(eq_filters):
        col = f.get("col")
        val = f.get("val")
        ci = bool(f.get("ci"))
        tr = bool(f.get("trim"))
        if not col or val is None:
            continue
        bind_name = f"eq_{i}"
        binds[bind_name] = val
        col_sql = quote_ident(col)
        left = col_sql
        right = f":{bind_name}"
        if tr:
            left = f"TRIM({left})"
            right = f"TRIM(:{bind_name})"
        if ci:
            left = f"UPPER({left})"
            right = f"UPPER({right})"
        where.append(f"{left} = {right}")
    return where


def _build_fts_where(
    tokens: list[str],
    fts_cols: list[str],
    op: str,
    binds: dict,
    bind_offset: int = 0,
) -> list[str]:
    groups = []
    for idx, tok in enumerate(tokens):
        t = tok.strip()
        if not t:
            continue
        bname = f"fts_{idx + bind_offset}"
        binds[bname] = f"%{t}%"
        ors = []
        for c in fts_cols:
            ors.append(f"UPPER(NVL({quote_ident(c)},'')) LIKE UPPER(:{bname})")
        groups.append("(" + " OR ".join(ors) + ")")
    if not groups:
        return []
    if (op or "OR").upper() == "AND":
        return ["(" + " AND ".join(groups) + ")"]
    return ["(" + " OR ".join(groups) + ")"]


@rate_bp.route("/dw/rate", methods=["POST"])
def rate():
    payload = request.get_json(force=True, silent=True) or {}
    inquiry_id = payload.get("inquiry_id")
    rating = payload.get("rating")
    comment = payload.get("comment") or ""

    hints_intent = parse_rate_comment(comment)

    measure_sql = build_measure_sql()

    binds: dict = {}
    where_parts: list[str] = []
    order_by: str | None = None
    sort_desc: bool | None = None

    fts_enabled = False
    fts_cols_dbg = []
    if hints_intent.get("full_text_search") and hints_intent.get("fts_tokens"):
        fts_cols = _select_rate_fts_columns()
        fts_cols_dbg = fts_cols[:]
        where_parts += _build_fts_where(
            hints_intent["fts_tokens"],
            fts_cols,
            hints_intent.get("fts_operator", "OR"),
            binds,
        )
        fts_enabled = True

    if hints_intent.get("eq_filters"):
        where_parts += _build_eq_where(hints_intent["eq_filters"], binds)

    group_by_raw = hints_intent.get("group_by")
    group_by = _resolve_group_by_col(group_by_raw) if group_by_raw else None
    gross_flag = hints_intent.get("gross")

    if hints_intent.get("sort_by"):
        order_by = hints_intent["sort_by"].strip()
    if "sort_desc" in hints_intent and hints_intent["sort_desc"] is not None:
        sort_desc = bool(hints_intent["sort_desc"])

    if order_by is None:
        order_by = "REQUEST_DATE"
        sort_desc = True

    table = '"Contract"'
    final_sql: str | None

    if group_by:
        gb = quote_ident(group_by)
        if gross_flag is True:
            select_cols = f"{gb} AS GROUP_KEY, SUM({measure_sql}) AS MEASURE, COUNT(*) AS CNT"
            default_order_col = "MEASURE"
        elif gross_flag is False:
            select_cols = f"{gb} AS GROUP_KEY, COUNT(*) AS CNT"
            default_order_col = "CNT"
        else:
            select_cols = f"{gb} AS GROUP_KEY, COUNT(*) AS CNT"
            default_order_col = "CNT"
        where_sql = " WHERE " + " AND ".join(where_parts) if where_parts else ""
        order_col = safe_upper((order_by or default_order_col).strip()) or default_order_col
        if order_col == "REQUEST_DATE":
            order_col = default_order_col
        if order_col not in ("MEASURE", "CNT"):
            order_col = quote_ident(order_col)
        direction = "DESC" if (sort_desc is True or sort_desc is None) else "ASC"
        final_sql = (
            f"SELECT {select_cols}\n"
            f"FROM {table}{where_sql}\n"
            f"GROUP BY {gb}\n"
            f"ORDER BY {order_col} {direction}"
        )
    else:
        where_sql = " WHERE " + " AND ".join(where_parts) if where_parts else ""
        direction = "DESC" if (sort_desc is True or sort_desc is None) else "ASC"
        final_sql = f"SELECT * FROM {table}{where_sql}\nORDER BY {quote_ident(order_by)} {direction}"

    final_sql = strip_double_order_by(final_sql)

    debug = {
        "fts": {
            "enabled": bool(fts_enabled),
            "tokens": hints_intent.get("fts_tokens") if fts_enabled else None,
            "columns": fts_cols_dbg if fts_enabled else None,
            "binds": list(binds.keys()) if fts_enabled else None,
            "error": None,
        },
        "intent": {
            "agg": None if not group_by else ("count" if gross_flag is not True else "sum"),
            "date_column": "OVERLAP",
            "eq_filters": hints_intent.get("eq_filters") or [],
            "group_by": [group_by] if group_by else [],
            "measure_sql": measure_sql,
        },
        "validation": {
            "ok": True,
            "errors": [],
            "binds": list(binds.keys()),
            "bind_names": list(binds.keys()),
        },
    }

    try:
        store = LearningStore()
        if rating is not None:
            if rating >= 4:
                store.save_example(
                    ExampleRecord(
                        inquiry_id=inquiry_id,
                        question=payload.get("question") or "",
                        sql=final_sql,
                        created_at=datetime.utcnow(),
                    )
                )
            elif rating <= 2 and comment:
                store.save_patch(
                    PatchRecord(
                        inquiry_id=inquiry_id,
                        comment=comment,
                        produced_sql=final_sql,
                        created_at=datetime.utcnow(),
                    )
                )
    except Exception as e:  # pragma: no cover - defensive logging path
        debug["learning_store_error"] = str(e)

    return jsonify(
        {
            "ok": True,
            "inquiry_id": inquiry_id,
            "sql": final_sql,
            "meta": {
                "attempt_no": 2,
                "binds": binds,
                "clarifier_intent": debug["intent"],
                "fts": debug["fts"],
                "rate_hints": {
                    "comment_present": bool(comment),
                    "eq_filters": len(hints_intent.get("eq_filters") or []),
                    "group_by": [group_by] if group_by else None,
                    "order_by_applied": True,
                    "where_applied": bool(where_parts),
                },
            },
            "debug": debug,
            "rows": [],
            "retry": True,
        }
    )
