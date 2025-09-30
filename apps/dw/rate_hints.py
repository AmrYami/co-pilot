import re
from typing import Dict, List, Optional, Tuple

from apps.dw.contracts.synonyms import (
    build_request_type_filter_sql,
    get_request_type_synonyms,
)


class RateHints:
    """Structured hints extracted from /dw/rate comment."""

    def __init__(
        self,
        where_sql: Optional[str] = None,
        where_binds: Optional[Dict[str, object]] = None,
        order_by_sql: Optional[str] = None,
        group_by_cols: Optional[List[str]] = None,
    ):
        self.where_sql = where_sql
        self.where_binds = where_binds or {}
        self.order_by_sql = order_by_sql
        self.group_by_cols = group_by_cols or []


def _slug(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", s.lower())


# Canonical columns for the Contract table (extend as needed).
_CONTRACT_COLS = [
    "CONTRACT_ID",
    "CONTRACT_OWNER",
    "CONTRACT_STAKEHOLDER_1",
    "CONTRACT_STAKEHOLDER_2",
    "CONTRACT_STAKEHOLDER_3",
    "CONTRACT_STAKEHOLDER_4",
    "CONTRACT_STAKEHOLDER_5",
    "CONTRACT_STAKEHOLDER_6",
    "CONTRACT_STAKEHOLDER_7",
    "CONTRACT_STAKEHOLDER_8",
    "OWNER_DEPARTMENT",
    "DEPARTMENT_OUL",
    "CONTRACT_VALUE_NET_OF_VAT",
    "VAT",
    "CONTRACT_PURPOSE",
    "CONTRACT_SUBJECT",
    "START_DATE",
    "END_DATE",
    "DURATION",
    "ENTITY",
    "LEGAL_NAME_OF_THE_COMPANY",
    "REQUEST_ID",
    "REQUEST_DATE",
    "CONTRACT_STATUS",
    "REQUEST_TYPE",
    "REQUESTER",
    "ENTITY_NO",
]
_SLUG_TO_CANON = {_slug(c): c for c in _CONTRACT_COLS}

# Some common textual synonyms -> canonical column
_SYNONYMS = {
    "requesttype": "REQUEST_TYPE",
    "request date": "REQUEST_DATE",
    "request_date": "REQUEST_DATE",
    "ownerdepartment": "OWNER_DEPARTMENT",
    "departmentoul": "DEPARTMENT_OUL",
    "entityno": "ENTITY_NO",
    "entity number": "ENTITY_NO",
    "status": "CONTRACT_STATUS",
    "contractstatus": "CONTRACT_STATUS",
    "subject": "CONTRACT_SUBJECT",
    "purpose": "CONTRACT_PURPOSE",
    "netvalue": "CONTRACT_VALUE_NET_OF_VAT",
    "net": "CONTRACT_VALUE_NET_OF_VAT",
}


def _canon_col(name: str) -> Optional[str]:
    """Normalize a user-provided column token to canonical DB column."""
    s = name.strip()
    # Try synonyms mapping first
    key = _slug(_SYNONYMS.get(s.lower(), s))
    return _SLUG_TO_CANON.get(key)


def _split_list_items(raw: str) -> List[str]:
    items: List[str] = []
    buf: List[str] = []
    quote: Optional[str] = None
    for ch in raw:
        if ch in ("'", '"'):
            if quote is None:
                quote = ch
                continue
            if quote == ch:
                quote = None
                continue
        if ch == "," and quote is None:
            item = "".join(buf).strip()
            if item:
                items.append(item.strip('"\''))
            buf = []
            continue
        buf.append(ch)
    if buf:
        item = "".join(buf).strip()
        if item:
            items.append(item.strip('"\''))
    return items


def _has_where(sql_upper: str) -> bool:
    return " WHERE " in sql_upper or sql_upper.startswith("WHERE ") or "\nWHERE " in sql_upper


def _find_insert_position(sql_upper: str) -> Optional[int]:
    for kw in [" GROUP BY ", " ORDER BY ", " FETCH FIRST ", "\nGROUP BY ", "\nORDER BY ", "\nFETCH "]:
        pos = sql_upper.find(kw)
        if pos != -1:
            return pos
    return None


def append_where(sql: str, where_sql: str) -> str:
    """Append a WHERE fragment safely into an existing SQL statement."""
    upper = sql.upper()
    insert_pos = _find_insert_position(upper)
    if _has_where(upper):
        if insert_pos is None:
            return f"{sql}\nAND {where_sql}"
        return f"{sql[:insert_pos]}\nAND {where_sql}\n{sql[insert_pos:]}"
    if insert_pos is None:
        return f"{sql}\nWHERE {where_sql}"
    return f"{sql[:insert_pos]}\nWHERE {where_sql}\n{sql[insert_pos:]}"


def replace_or_add_order_by(sql: str, order_by_sql: str) -> str:
    """Replace existing ORDER BY or add a new one, preserving trailing FETCH clauses."""
    upper = sql.upper()
    ob_pos = upper.find(" ORDER BY ")
    fetch_pos = upper.find(" FETCH FIRST ")
    if ob_pos == -1:
        if fetch_pos != -1:
            return f"{sql[:fetch_pos]}\n{order_by_sql}\n{sql[fetch_pos:]}"
        return f"{sql}\n{order_by_sql}"
    if fetch_pos == -1 or fetch_pos < ob_pos:
        return sql[:ob_pos] + " " + order_by_sql
    return sql[:ob_pos] + " " + order_by_sql + "\n" + sql[fetch_pos:]


def parse_rate_hints(comment: Optional[str], settings_get_json=None) -> RateHints:
    """
    Parse micro-language in /dw/rate comment. Examples:
      filter: REQUEST_TYPE ~ renew; order_by: REQUEST_DATE desc;
      filter: REQUEST TYPE = "Renewal";
      filter: CONTRACT_STATUS in ('Active','Pending'); order_by: NET desc;
    Supported ops: =, !=, ~ (contains), startswith, endswith, in (comma or SQL-like list)
    """
    hints = RateHints()
    if not comment:
        return hints

    text = comment.strip()
    # Split by semicolons into directives
    parts = [p.strip() for p in re.split(r";\s*", text) if p.strip()]
    where_clauses: List[str] = []
    binds: Dict[str, object] = {}
    bind_idx = 0
    reqtype_synonyms: Optional[Dict[str, List[str]]] = None
    reqtype_filter_count = 0

    def new_bind(val):
        nonlocal bind_idx
        k = f"rh_{bind_idx}"
        bind_idx += 1
        binds[k] = val
        return k

    def ensure_reqtype_synonyms() -> Dict[str, List[str]]:
        nonlocal reqtype_synonyms
        if reqtype_synonyms is None:
            reqtype_synonyms = get_request_type_synonyms(settings_get_json)
        return reqtype_synonyms

    def apply_reqtype_filter(val: str) -> Tuple[str, Dict[str, object]]:
        nonlocal reqtype_filter_count
        syn_map = ensure_reqtype_synonyms()
        prefix = f"rh_reqtype{reqtype_filter_count}"
        reqtype_filter_count += 1
        return build_request_type_filter_sql(val, syn_map, use_like=True, bind_prefix=prefix)

    def parse_filter(expr: str):
        """
        Accept formats like:
          COL = value
          COL != value
          COL ~ value         (LIKE %value%)
          COL startswith value
          COL endswith value
          COL in (v1, v2, v3) or "v1,v2,v3"
        Value can be quoted or bare word.
        """
        # Normalize "REQUEST TYPE" -> "REQUEST_TYPE"
        expr_norm = re.sub(r"\s+", " ", expr).strip()

        # IN (...)
        m_in = re.match(r"(?i)\s*(.+?)\s+in\s*\((.+)\)\s*$", expr_norm)
        if m_in:
            col_raw, list_raw = m_in.group(1).strip(), m_in.group(2).strip()
            col = _canon_col(col_raw) or _canon_col(col_raw.replace(" ", "_"))
            if not col:
                return
            items = _split_list_items(list_raw)
            if not items:
                return
            bind_keys = []
            for it in items:
                bk = new_bind(it)
                bind_keys.append(f":{bk}")
            where_clauses.append(f"UPPER({col}) IN ({', '.join(f'UPPER({b})' for b in bind_keys)})")
            return

        m_in_alt = re.match(r"(?i)\s*(.+?)\s+in\s+(['\"])(.+)\2\s*$", expr_norm)
        if m_in_alt:
            col_raw = m_in_alt.group(1).strip()
            list_raw = m_in_alt.group(3).strip()
            col = _canon_col(col_raw) or _canon_col(col_raw.replace(" ", "_"))
            if not col:
                return
            items = _split_list_items(list_raw)
            if not items:
                return
            bind_keys = []
            for it in items:
                bk = new_bind(it)
                bind_keys.append(f":{bk}")
            where_clauses.append(f"UPPER({col}) IN ({', '.join(f'UPPER({b})' for b in bind_keys)})")
            return

        # startswith / endswith
        m_sw = re.match(r"(?i)\s*(.+?)\s+startswith\s+(.+)$", expr_norm)
        if m_sw:
            col_raw, val_raw = m_sw.group(1).strip(), m_sw.group(2).strip().strip('\'"')
            col = _canon_col(col_raw) or _canon_col(col_raw.replace(" ", "_"))
            if not col:
                return
            bk = new_bind(f"{val_raw}%")
            where_clauses.append(f"UPPER({col}) LIKE UPPER(:{bk})")
            return

        m_ew = re.match(r"(?i)\s*(.+?)\s+endswith\s+(.+)$", expr_norm)
        if m_ew:
            col_raw, val_raw = m_ew.group(1).strip(), m_ew.group(2).strip().strip('\'"')
            col = _canon_col(col_raw) or _canon_col(col_raw.replace(" ", "_"))
            if not col:
                return
            bk = new_bind(f"%{val_raw}")
            where_clauses.append(f"UPPER({col}) LIKE UPPER(:{bk})")
            return

        # contains (~)
        m_like = re.match(r"(?i)\s*(.+?)\s*~\s*(.+)$", expr_norm)
        if m_like:
            col_raw, val_raw = m_like.group(1).strip(), m_like.group(2).strip().strip('\'"')
            col = _canon_col(col_raw) or _canon_col(col_raw.replace(" ", "_"))
            if not col:
                return
            if col == "REQUEST_TYPE":
                frag, extra_binds = apply_reqtype_filter(val_raw)
                where_clauses.append(frag)
                binds.update(extra_binds)
            else:
                bk = new_bind(f"%{val_raw}%")
                where_clauses.append(f"UPPER({col}) LIKE UPPER(:{bk})")
            return

        # equality / inequality
        m_eq = re.match(r"(?i)\s*(.+?)\s*(=|==|!=)\s*(.+)$", expr_norm)
        if m_eq:
            col_raw, op, val_raw = m_eq.group(1).strip(), m_eq.group(2), m_eq.group(3).strip().strip('\'"')
            col = _canon_col(col_raw) or _canon_col(col_raw.replace(" ", "_"))
            if not col:
                return
            if col == "REQUEST_TYPE" and op != "!=":
                frag, extra_binds = apply_reqtype_filter(val_raw)
                where_clauses.append(frag)
                binds.update(extra_binds)
            else:
                bk = new_bind(val_raw)
                if op == "!=":
                    where_clauses.append(f"UPPER({col}) <> UPPER(:{bk})")
                else:
                    where_clauses.append(f"UPPER({col}) = UPPER(:{bk})")
            return

    for p in parts:
        if p.lower().startswith("filter:"):
            filt = p[len("filter:") :].strip()
            # allow multiple comma-separated expressions inside filter:
            for sub in [
                x.strip()
                for x in re.split(r",(?=(?:[^'\"]|'[^']*'|\"[^\"]*\")*$)", filt)
                if x.strip()
            ]:
                parse_filter(sub)
        elif p.lower().startswith("order_by:"):
            order_expr = p[len("order_by:") :].strip()
            # Expect "COL asc|desc" (desc default if absent "lowest" mapping can be handled outside)
            m = re.match(r"(?i)\s*(.+?)(\s+(asc|desc))?\s*$", order_expr)
            if m:
                col_raw, _, dir_tok = m.group(1).strip(), m.group(2), (m.group(3) or "desc")
                col = _canon_col(col_raw) or _canon_col(col_raw.replace(" ", "_"))
                if col:
                    hints.order_by_sql = f"ORDER BY {col} {dir_tok.upper()}"
        elif p.lower().startswith("group_by:"):
            grp = p[len("group_by:") :].strip()
            cols = [c.strip() for c in grp.split(",") if c.strip()]
            canon: List[str] = []
            for c in cols:
                col = _canon_col(c) or _canon_col(c.replace(" ", "_"))
                if col:
                    canon.append(col)
            if canon:
                hints.group_by_cols = canon

    if where_clauses:
        hints.where_sql = "(" + ") AND (".join(where_clauses) + ")"
    hints.where_binds = binds
    return hints
