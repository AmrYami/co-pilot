import re
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Tuple, TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - import cycle guard for type checkers
    from .intent import NLIntent


@dataclass
class EqFilter:
    col: str
    value: str
    ci: bool = True
    trim: bool = True


@dataclass
class LikeFilter:
    col: str
    pattern: str
    ci: bool = True
    trim: bool = True


@dataclass
class RateHints:
    eq_filters: List[EqFilter] = field(default_factory=list)
    like_filters: List[LikeFilter] = field(default_factory=list)
    group_by: List[str] = field(default_factory=list)
    order_by: List[Tuple[str, str]] = field(default_factory=list)  # (expr, 'asc'|'desc')
    measure: Optional[str] = None  # 'gross' | 'net' | None
    top_n: Optional[int] = None


_BASIC_EQ_RE = re.compile(r"\b(?:eq|filter)\s*:\s*([A-Z0-9_ ]+)\s*=\s*(.+?)(?:;|$)", re.IGNORECASE)
_BASIC_FTS_RE = re.compile(r"\bfts\s*:\s*(.+?)(?:;|$)", re.IGNORECASE)
_BASIC_GBY_RE = re.compile(r"\bgroup_by\s*:\s*([A-Z0-9_, ]+)", re.IGNORECASE)
_BASIC_OBY_RE = re.compile(r"\border_by\s*:\s*([A-Z0-9_]+)(?:\s+(asc|desc))?", re.IGNORECASE)
_BASIC_FLAGS_RE = re.compile(r"\(([^)]*)\)")


def _parse_basic_flags(raw_val: str) -> Tuple[str, bool, bool]:
    value = (raw_val or "").strip()
    ci = False
    trim = False
    match = _BASIC_FLAGS_RE.search(value)
    if match:
        raw_flags = match.group(1) or ""
        flags = [frag.strip().lower() for frag in raw_flags.split(",") if frag.strip()]
        ci = any(flag in {"ci", "case_insensitive"} for flag in flags)
        trim = "trim" in flags
        value = _BASIC_FLAGS_RE.sub("", value).strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
        value = value[1:-1]
    return value, ci, trim


def parse_rate_comment(comment: str) -> Dict[str, Any]:
    """Parse a free-form /dw/rate comment into structured filter hints."""

    hints: Dict[str, Any] = {
        "eq_filters": [],
        "fts_tokens": [],
        "fts_operator": "OR",
        "group_by": None,
        "sort_by": None,
        "sort_desc": None,
    }
    if not comment:
        return hints

    fts_match = _BASIC_FTS_RE.search(comment)
    if fts_match:
        raw_tokens = fts_match.group(1)
        tokens = [tok.strip() for tok in raw_tokens.split("|") if tok.strip()]
        hints["fts_tokens"] = tokens

    for col_raw, val_raw in _BASIC_EQ_RE.findall(comment or ""):
        col = col_raw.strip().upper().replace(" ", "_")
        value, ci, trim = _parse_basic_flags(val_raw)
        if not col or value == "":
            continue
        hints["eq_filters"].append({
            "col": col,
            "val": value,
            "ci": ci,
            "trim": trim,
            "op": "eq",
        })

    gby_match = _BASIC_GBY_RE.search(comment)
    if gby_match:
        cols = [frag.strip().upper().replace(" ", "_") for frag in gby_match.group(1).split(",") if frag.strip()]
        hints["group_by"] = cols or None

    oby_match = _BASIC_OBY_RE.search(comment)
    if oby_match:
        hints["sort_by"] = (oby_match.group(1) or "").strip().upper().replace(" ", "_") or None
        direction = (oby_match.group(2) or "DESC").strip().lower()
        hints["sort_desc"] = direction != "asc"

    return hints




_flags_re = re.compile(r"\(([^)]*)\)\s*$")


@dataclass
class StrictFilter:
    col: str
    value: str
    ci: bool = False
    trim: bool = False


@dataclass
class StrictOrderHint:
    expr: str
    desc: bool


@dataclass
class StrictRateHints:
    filters: List[StrictFilter] = field(default_factory=list)
    group_by: List[str] = field(default_factory=list)
    order_by: List[StrictOrderHint] = field(default_factory=list)

    def is_empty(self) -> bool:
        return not (self.filters or self.group_by or self.order_by)


def _parse_flags(s: str) -> Tuple[bool, bool]:
    ci = True
    trim = True
    m = _flags_re.search(s)
    if not m:
        return ci, trim
    flags = {t.strip().lower() for t in m.group(1).split(",")}
    ci = ("ci" in flags) or ("case_insensitive" in flags)
    trim = ("trim" in flags)
    return ci, trim


def _clean_value_literal(val: str) -> str:
    """Drop trailing hints like (ci, trim) or ; comments from a literal."""

    if not val:
        return val
    val = re.split(r"\s*\(", val, 1)[0]
    val = val.split(";", 1)[0]
    return val.strip().strip('"').strip("'")


def parse_rate_comment_legacy(comment: str) -> RateHints:
    """
    Very small grammar:
      filter: COL = 'value' (ci, trim);
      filter: COL ~ token;  -> LIKE %token%
      group_by: COL[, COL...];
      order_by: EXPR asc|desc;
      measure: gross|net;
      top: N;
    """
    hints = RateHints()
    # split by ';'
    parts = [p.strip() for p in comment.split(";") if p.strip()]
    for p in parts:
        if p.lower().startswith("filter:"):
            body = p.split(":", 1)[1].strip()
            # try equality first: COL = 'value' [flags]
            m = re.match(r"([A-Za-z_][A-Za-z0-9_ ]*?)\s*=\s*(.+)$", body)
            if m:
                col = m.group(1).strip()
                raw_val = m.group(2).strip()
                ci, trim = _parse_flags(raw_val)
                # strip flags from tail
                raw_val = _flags_re.sub("", raw_val).strip()
                raw_val = _clean_value_literal(raw_val)
                hints.eq_filters.append(EqFilter(col=col, value=raw_val, ci=ci, trim=trim))
                continue
            # like: COL ~ token
            m = re.match(r"([A-Za-z_][A-Za-z0-9_ ]*?)\s*~\s*(.+)$", body)
            if m:
                col = m.group(1).strip()
                token = m.group(2).strip()
                ci, trim = _parse_flags(token)
                token = _clean_value_literal(_flags_re.sub("", token).strip())
                hints.like_filters.append(LikeFilter(col=col, pattern=token, ci=ci, trim=trim))
                continue

        elif p.lower().startswith("group_by:"):
            cols = p.split(":", 1)[1]
            hints.group_by = [c.strip().upper().replace(" ", "_") for c in cols.split(",") if c.strip()]

        elif p.lower().startswith("order_by:"):
            body = p.split(":", 1)[1].strip()
            m = re.match(r"(.+?)\s+(asc|desc)$", body, re.IGNORECASE)
            if m:
                hints.order_by.append((m.group(1).strip(), m.group(2).lower()))
            else:
                hints.order_by.append((body, "desc"))

        elif p.lower().startswith("measure:"):
            val = p.split(":", 1)[1].strip().lower()
            if val in ("gross", "net"):
                hints.measure = val

        elif p.lower().startswith("top:"):
            try:
                hints.top_n = int(p.split(":", 1)[1].strip())
            except ValueError:
                pass
    return hints


parse_rate_comment_rate_hints = parse_rate_comment_legacy


_EQ_RE = re.compile(
    r"""(?ix)
    \b(?:filter|where)\s*:\s*
    (?P<col>[A-Z0-9_.]+)\s*
    (?:=|==|eq)\s*
    (?P<val>
        "(?P<dq>[^"\\]*(?:\\.[^"\\]*)*)"  # double-quoted
        |'(?:\\'|[^'])*'                       # single-quoted
        |[A-Z0-9_\-./@]+                        # bare token
    )
    \s*
    (?P<trail>\([^)]*\))?                     # optional flags like (ci, trim)
    \s*;?
    """
)

_FLAGS_RE = re.compile(r"(?i)\bflags\s*:\s*(?P<flags>[^;]+)")
_GROUP_RE = re.compile(r"(?i)\bgroup_by\s*:\s*([A-Z0-9_,\s]+);?")
_ORDER_RE = re.compile(r"(?i)\border_by\s*:\s*([A-Z0-9_]+)\s+(asc|desc)\s*;?")


def _strip_quotes(raw: str) -> str:
    text = raw.strip()
    if len(text) >= 2 and text[0] in {'"', "'"} and text[-1] == text[0]:
        body = text[1:-1]
        if text[0] == '"':
            body = body.replace("\\\"", '"')
        else:
            body = body.replace("\\'", "'")
        return body
    return text


def _extract_flag_values(flag_blob: str) -> Dict[str, bool]:
    values = {frag.strip().lower() for frag in flag_blob.split(",") if frag.strip()}
    return {"ci": "ci" in values or "case_insensitive" in values, "trim": "trim" in values}


def parse_rate_comment_strict(comment: Optional[str]) -> StrictRateHints:
    hints = StrictRateHints()
    text = (comment or "").strip()
    if not text:
        return hints

    global_flags = {"ci": False, "trim": False}
    fm = _FLAGS_RE.search(text)
    if fm:
        global_flags.update(_extract_flag_values(fm.group("flags")))

    for match in _EQ_RE.finditer(text):
        col = (match.group("col") or "").strip().upper()
        if not col:
            continue
        raw_val = match.group("val") or ""
        value = _strip_quotes(raw_val)
        flags = dict(global_flags)
        trail = match.group("trail") or ""
        if trail:
            flags.update(_extract_flag_values(trail.strip("()")))
        hints.filters.append(
            StrictFilter(col=col, value=value, ci=flags["ci"], trim=flags["trim"])
        )

    grp_match = _GROUP_RE.search(text)
    if grp_match:
        cols = [c.strip().upper() for c in grp_match.group(1).split(",") if c.strip()]
        if cols:
            hints.group_by.extend(cols)

    order_match = _ORDER_RE.search(text)
    if order_match:
        expr = order_match.group(1).strip().upper()
        direction = order_match.group(2).strip().lower() == "desc"
        if expr:
            hints.order_by.append(StrictOrderHint(expr=expr, desc=direction))

    return hints


EQ_CMD = re.compile(r"(?i)\beq\s*:\s*([A-Z0-9_ ]+)\s*=\s*(.+?)(?:;|$)")
FTS_CMD = re.compile(r"(?i)\bfts\s*:\s*(.+?)(?:;|$)")
GB_CMD = re.compile(r"(?i)\bgroup_by\s*:\s*([A-Z0-9_, ]+)(?:;|$)")
ORD_CMD = re.compile(r"(?i)\border_by\s*:\s*([A-Z0-9_ ]+?)(?:\s+(asc|desc))?(?:;|$)")
GROSS_CMD = re.compile(r"(?i)\bgross\s*:\s*(true|false)(?:;|$)")
FLAGS = re.compile(r"\((?P<flags>[^)]*)\)$")


def _parse_flags(val: str) -> Tuple[str, bool, bool]:
    value = val.strip()
    ci = False
    trim = False
    match = FLAGS.search(value)
    if match:
        raw_flags = match.group("flags") or ""
        for flag in (frag.strip().lower() for frag in raw_flags.split(",") if frag.strip()):
            if flag == "ci" or flag == "case_insensitive":
                ci = True
            elif flag == "trim":
                trim = True
        value = value[: match.start()].strip()
    return value, ci, trim


def apply_rate_comment(intent: Dict[str, Any], comment: str) -> Dict[str, Any]:
    """Apply /dw/rate micro-language patches to an intent dictionary."""

    if not isinstance(intent, dict):
        intent = {}
    out = dict(intent)

    eq_filters: List[Dict[str, Any]] = list(out.get("eq_filters") or [])

    for match in EQ_CMD.finditer(comment or ""):
        col = (match.group(1) or "").strip()
        val_raw = (match.group(2) or "").strip()
        value, ci, trim = _parse_flags(val_raw)
        if col and value:
            eq_filters.append({"col": col, "val": value, "ci": ci, "trim": trim})

    out["eq_filters"] = eq_filters

    fts_match = FTS_CMD.search(comment or "")
    if fts_match:
        expr = (fts_match.group(1) or "").strip()
        tokens = [tok.strip() for tok in expr.split("|") if tok.strip()]
        if tokens:
            from apps.dw.fts import build_like_fts_where

            where, binds = build_like_fts_where(
                out.get("schema_key", "Contract"),
                [[tok] for tok in tokens],
                bind_prefix="fts",
            )
            out["fts"] = {"enabled": bool(where), "where": where, "binds": binds}
            out["full_text_search"] = True

    gb_match = GB_CMD.search(comment or "")
    if gb_match:
        cols = [c.strip().upper() for c in (gb_match.group(1) or "").split(",") if c.strip()]
        out["group_by"] = cols or None

    ord_match = ORD_CMD.search(comment or "")
    if ord_match:
        out["sort_by"] = (ord_match.group(1) or "").strip().upper()
        direction = (ord_match.group(2) or "DESC").strip().lower()
        out["sort_desc"] = direction != "asc"

    gross_match = GROSS_CMD.search(comment or "")
    if gross_match:
        out["gross"] = gross_match.group(1).strip().lower() == "true"

    return out


def merge_rate_comment_hints(
    intent: "NLIntent",
    hints: StrictRateHints,
    allowed_columns: Iterable[str],
) -> "NLIntent":
    if hints.is_empty():
        return intent

    allowed_map = {col.upper(): col.upper() for col in allowed_columns}

    try:
        merged = intent.copy(deep=True)  # type: ignore[attr-defined]
    except Exception:
        merged = intent

    existing_filters: List[Dict[str, Any]] = list(getattr(merged, "eq_filters", []) or [])
    for filt in hints.filters:
        canonical = allowed_map.get(filt.col.upper())
        if not canonical:
            continue
        existing_filters.append(
            {
                "col": canonical,
                "op": "eq",
                "val": filt.value,
                "ci": bool(filt.ci),
                "trim": bool(filt.trim),
            }
        )
    if existing_filters:
        merged.eq_filters = existing_filters  # type: ignore[attr-defined]

    if hints.group_by:
        merged.group_by = hints.group_by[0]

    if hints.order_by:
        first = hints.order_by[0]
        merged.sort_by = first.expr
        merged.sort_desc = first.desc

    return merged
