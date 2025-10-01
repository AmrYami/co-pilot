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


def parse_rate_comment(comment: str) -> RateHints:
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
