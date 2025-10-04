import re
from typing import Any, Dict, List, Optional, Tuple

from apps.dw.contracts.synonyms import (
    build_request_type_filter_sql,
    get_request_type_synonyms,
)

_SETTINGS_CACHE: Any = None


def _get_default_settings() -> Any:
    global _SETTINGS_CACHE
    if _SETTINGS_CACHE is not None:
        return _SETTINGS_CACHE
    try:
        from core.settings import Settings  # type: ignore

        _SETTINGS_CACHE = Settings(namespace="dw::common")
    except Exception:
        _SETTINGS_CACHE = None
    return _SETTINGS_CACHE


FTS_RE = re.compile(r"\bfts\s*:\s*([^\n;]+)", re.IGNORECASE)
FTS_COLS_RE = re.compile(r"\bfts-cols\s*:\s*([^\n;]+)", re.IGNORECASE)


def _normalize_list(text: str) -> Tuple[List[str], str]:
    raw = (text or "").strip()
    if not raw:
        return [], "OR"
    lowered = raw.lower()
    if "|" in raw:
        tokens = [part.strip() for part in raw.split("|")]
        op = "OR"
    elif "&" in raw:
        tokens = [part.strip() for part in raw.split("&")]
        op = "AND"
    elif " and " in lowered:
        tokens = [part.strip() for part in re.split(r"\band\b", raw, flags=re.IGNORECASE)]
        op = "AND"
    elif " or " in lowered:
        tokens = [part.strip() for part in re.split(r"\bor\b", raw, flags=re.IGNORECASE)]
        op = "OR"
    else:
        tokens = [raw]
        op = "OR"
    cleaned = [tok.strip(" '\"") for tok in tokens if tok and tok.strip(" '\"")]
    return cleaned, op


def _default_fts_columns(table_name: str = "Contract") -> List[str]:
    settings = _get_default_settings()
    raw: Any = {}
    if settings is None:
        raw = {}
    else:
        getter = getattr(settings, "get_json", None)
        if callable(getter):
            try:
                raw = getter("DW_FTS_COLUMNS", {})
            except TypeError:
                raw = getter("DW_FTS_COLUMNS")
        if raw is None:
            raw = {}
    columns: List[str] = []
    if isinstance(raw, dict):
        for key in (
            table_name,
            table_name.strip('"'),
            table_name.upper(),
            table_name.lower(),
            "*",
        ):
            vals = raw.get(key)
            if isinstance(vals, list):
                columns.extend(vals)
    elif isinstance(raw, list):
        columns.extend(raw)
    normalized: List[str] = []
    seen: set[str] = set()
    for col in columns:
        if not isinstance(col, str):
            continue
        stripped = col.strip().upper()
        if not stripped or stripped in seen:
            continue
        seen.add(stripped)
        normalized.append(stripped)
    return normalized


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
    fetch_match = re.search(r"\bFETCH\s+FIRST\b", upper)
    fetch_start = fetch_match.start() if fetch_match else len(sql)
    order_match = re.search(r"\bORDER\s+BY\b", upper)

    if order_match and order_match.start() < fetch_start:
        prefix = sql[: order_match.start()].rstrip()
    else:
        prefix = sql[:fetch_start].rstrip()

    suffix = sql[fetch_start:] if fetch_start < len(sql) else ""

    parts: List[str] = []
    if prefix:
        parts.append(prefix)
        if not prefix.endswith("\n"):
            parts.append("\n")
    parts.append(order_by_sql)

    if suffix:
        cleaned_suffix = suffix.lstrip("\n")
        if cleaned_suffix:
            parts.append("\n")
            parts.append(cleaned_suffix)

    return "".join(parts)


# --- Lightweight parser used by rate feedback comments ---------------------------------------

_EQ_CMD = re.compile(
    r"(?i)\bfilter:\s*([A-Z0-9_.]+)\s*=\s*(?:'([^']*)'|\"([^\"]*)\"|([^;\)]+?))(?=\s*(?:\(|;|$))"
)

FILTER_LIKE_RE = re.compile(
    r"(?i)filter:\s*([A-Z0-9_. \-]+?)\s*(?:~|like|ilike)\s*(?:'([^']*)'|\"([^\"]*)\"|([^;\)]+?))(?=\s*(?:\(|;|$))"
)


def _norm_col(col: str) -> str:
    """Normalize "REQUEST TYPE" -> "REQUEST_TYPE"."""

    return col.strip().upper().replace(" ", "_")


def parse_rate_comment(comment: str) -> Dict[str, Any]:
    """Parse /dw/rate comment micro-language into structured hints."""

    hints: Dict[str, Any] = {}
    text = comment or ""

    m_fts = FTS_RE.search(text)
    if m_fts:
        tokens, op = _normalize_list(m_fts.group(1))
        if tokens:
            hints["full_text_search"] = True
            hints["fts_tokens"] = tokens
            hints["fts_operator"] = op

    m_cols = FTS_COLS_RE.search(text)
    if m_cols:
        cols = [
            part.strip().upper()
            for part in re.split(r"[\s,]+", m_cols.group(1))
            if part.strip()
        ]
        if cols:
            hints["fts_columns"] = cols

    m = re.search(r"(?i)group_by\s*:\s*([A-Z0-9_, \-]+)", text)
    if m:
        hints["group_by"] = [
            _norm_col(x) for x in m.group(1).split(",") if x.strip()
        ]

    m = re.search(r"(?i)order_by\s*:\s*([A-Z0-9_]+)\s*(asc|desc)?", text)
    if m:
        col = _norm_col(m.group(1))
        desc = (m.group(2) or "DESC").strip().lower() == "desc"
        hints["order_by"] = (col, desc)

    eq_filters: List[Dict[str, Any]] = []
    seen_eq: set[tuple[str, str, bool, bool]] = set()

    for match in _EQ_CMD.finditer(text):
        col = _norm_col(match.group(1))
        raw_val = match.group(2) or match.group(3) or match.group(4) or ""
        val = raw_val.strip()
        tail = text[match.end() :]
        ci = False
        trim = False
        flag_match = re.match(r"\s*\(([^)]*)\)", tail)
        if flag_match:
            opts = {opt.strip().lower() for opt in flag_match.group(1).split(",") if opt.strip()}
            ci = "ci" in opts
            trim = "trim" in opts
        key = (col, val.lower(), ci, trim)
        if key in seen_eq:
            continue
        seen_eq.add(key)
        eq_filters.append(
            {
                "col": col,
                "op": "eq",
                "val": val,
                "ci": ci,
                "trim": trim,
            }
        )

    for match in FILTER_LIKE_RE.finditer(text):
        col = _norm_col(match.group(1))
        raw_val = match.group(2) or match.group(3) or match.group(4) or ""
        val = raw_val.strip()
        tail = text[match.end() :]
        ci = False
        trim = False
        flag_match = re.match(r"\s*\(([^)]*)\)", tail)
        if flag_match:
            opts = {opt.strip().lower() for opt in flag_match.group(1).split(",") if opt.strip()}
            ci = "ci" in opts
            trim = "trim" in opts
        eq_filters.append(
            {
                "col": col,
                "op": "like",
                "val": val,
                "ci": ci,
                "trim": trim,
            }
        )

    if eq_filters:
        hints["eq_filters"] = eq_filters

    return hints


def _eq_filter_signature(filter_spec: Dict[str, Any]) -> Optional[Tuple[str, str, bool, bool, Any]]:
    col = (filter_spec.get("col") or filter_spec.get("column") or "").strip().upper()
    if not col:
        return None
    op = (filter_spec.get("op") or "eq").strip().lower()
    ci = bool(filter_spec.get("ci"))
    trim = bool(filter_spec.get("trim"))
    synonyms = filter_spec.get("synonyms") if isinstance(filter_spec.get("synonyms"), dict) else None
    if synonyms:
        equals = tuple(sorted(str(v).strip() for v in synonyms.get("equals", []) if v))
        prefix = tuple(sorted(str(v).strip() for v in synonyms.get("prefix", []) if v))
        contains = tuple(sorted(str(v).strip() for v in synonyms.get("contains", []) if v))
        value_sig: Any = ("syn", equals, prefix, contains)
    else:
        raw_val = (
            filter_spec.get("val")
            if filter_spec.get("val") is not None
            else filter_spec.get("value")
            if filter_spec.get("value") is not None
            else filter_spec.get("pattern")
        )
        if isinstance(raw_val, str):
            value_sig = raw_val.strip()
        elif isinstance(raw_val, (list, tuple, set)):
            value_sig = tuple(str(v).strip() for v in raw_val)
        else:
            value_sig = raw_val
    return col, op, ci, trim, value_sig


def merge_eq_filters(intent: Dict[str, Any], new_eq_filters: Optional[List[Dict[str, Any]]]) -> None:
    if not isinstance(intent, dict):
        return
    existing = intent.get("eq_filters")
    base: List[Dict[str, Any]] = list(existing) if isinstance(existing, list) else []
    combined = base + (list(new_eq_filters) if new_eq_filters else [])
    if not combined:
        intent["eq_filters"] = []
        return
    merged: List[Dict[str, Any]] = []
    seen: set[Tuple[str, str, bool, bool, Any]] = set()
    for spec in combined:
        if not isinstance(spec, dict):
            continue
        signature = _eq_filter_signature(spec)
        if signature is None:
            continue
        if signature in seen:
            continue
        seen.add(signature)
        merged.append(spec)
    intent["eq_filters"] = merged


def apply_rate_hints(intent: Dict[str, Any], comment: str) -> Dict[str, Any]:
    """Merge parsed hints into an intent dictionary without dropping existing context."""

    hints = parse_rate_comment(comment or "")

    eq_entries: List[Dict[str, Any]] = []
    if hints.get("eq_filters"):
        for filt in hints["eq_filters"]:
            entry = {
                "col": filt["col"],
                "val": filt["val"],
                "ci": filt.get("ci", False),
                "trim": filt.get("trim", False),
            }
            if filt.get("op") == "like":
                entry["op"] = "like"
            else:
                entry["synonyms"] = {
                    "equals": [filt["val"]],
                    "prefix": [],
                    "contains": [],
                }
            eq_entries.append(entry)
    if eq_entries:
        merge_eq_filters(intent, eq_entries)

    if hints.get("full_text_search"):
        intent["full_text_search"] = True
        tokens = hints.get("fts_tokens") or []
        if tokens:
            intent["fts_tokens"] = tokens
            intent["fts_operator"] = hints.get("fts_operator") or "OR"
        elif hints.get("fts_operator") and not intent.get("fts_operator"):
            intent["fts_operator"] = hints["fts_operator"]
        cols = hints.get("fts_columns")
        if cols:
            norm_cols = [col.strip().upper() for col in cols if isinstance(col, str) and col.strip()]
            if norm_cols:
                intent["fts_columns"] = norm_cols
        elif not intent.get("fts_columns"):
            defaults = _default_fts_columns()
            if defaults:
                intent["fts_columns"] = defaults

    if hints.get("group_by"):
        intent["group_by"] = ",".join(hints["group_by"])

    if hints.get("order_by"):
        col, desc = hints["order_by"]
        if col in {"TOTAL_GROSS", "GROSS", "SUM_GROSS"}:
            intent["sort_by"] = "TOTAL_GROSS"
            intent["sort_desc"] = desc
            intent["gross"] = True
        else:
            intent["sort_by"] = col
            intent["sort_desc"] = desc

    if intent.get("group_by"):
        intent["agg"] = None

    return intent


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
            if op in ("=", "=="):
                return  # handled by strict parser / builder
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
