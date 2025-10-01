import re
from dataclasses import dataclass, field
from typing import List, Tuple, Optional


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
                # strip quotes if any
                if (raw_val.startswith("'") and raw_val.endswith("'")) or \
                   (raw_val.startswith('"') and raw_val.endswith('"')):
                    raw_val = raw_val[1:-1]
                hints.eq_filters.append(EqFilter(col=col, value=raw_val, ci=ci, trim=trim))
                continue
            # like: COL ~ token
            m = re.match(r"([A-Za-z_][A-Za-z0-9_ ]*?)\s*~\s*(.+)$", body)
            if m:
                col = m.group(1).strip()
                token = m.group(2).strip()
                ci, trim = _parse_flags(token)
                token = _flags_re.sub("", token).strip().strip("'\"")
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
