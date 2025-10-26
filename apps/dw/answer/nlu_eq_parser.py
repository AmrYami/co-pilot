import re
from typing import Dict, List, Tuple
from apps.dw.lib.sql_utils import is_email, is_phone


def _tok(s: str) -> str:
    return (s or "").strip()


def parse_from_question(q: str, allowed_cols: List[str]) -> Tuple[List[Tuple[str, List[str]]], List[List[Tuple[str, List[str]]]]]:
    """
    Returns eq_filters (same-column values aggregated) and or_groups (cross-column OR buckets).
    """
    return parse_with_or(q, allowed_cols)


def parse_with_or(question: str, allowed_cols: List[str]) -> Tuple[List[Tuple[str, List[str]]], List[List[Tuple[str, List[str]]]]]:
    """
    Advanced parser returning both same-column IN filters and cross-column OR groups.
    Keeps values as-is (callers may normalize/canonicalize later).
    """
    text = question or ""
    if not allowed_cols:
        return [], []
    # Build a regex that recognizes any allowed column on the left-hand side
    cols = sorted([str(c).strip() for c in allowed_cols if str(c).strip()], key=len, reverse=True)
    col_alt = "|".join(map(re.escape, cols))
    col_re = re.compile(rf"(?i)\b({col_alt})\b\s*=\s*")

    pos = 0
    segments: List[Tuple[str, str]] = []
    while True:
        m = col_re.search(text, pos)
        if not m:
            break
        col = m.group(1)
        val_start = m.end()
        next_m = col_re.search(text, val_start)
        seg = text[val_start: next_m.start()] if next_m else text[val_start:]
        segments.append((col, seg))
        pos = next_m.start() if next_m else len(text)

    eq_map: Dict[str, List[str]] = {}
    or_groups: List[List[Tuple[str, List[str]]]] = []
    for col, seg in segments:
        parts = re.split(r"(?i)\bOR\b", seg)
        bucket: List[Tuple[str, List[str]]] = []
        for part in parts:
            part = (part or "").strip(" ,;")
            if not part:
                continue
            # If another column assignment appears in this OR chain, treat as cross-column
            m2 = col_re.search(part)
            if m2:
                col2 = m2.group(1)
                v2 = part[m2.end():].strip(" ,;")
                if v2:
                    bucket.append((col2, [v2]))
            else:
                bucket.append((col, [part]))

        cols_in_bucket = {c for c, _ in bucket}
        if len(cols_in_bucket) > 1:
            merged: Dict[str, List[str]] = {}
            for c, vs in bucket:
                merged.setdefault(c, []).extend([(v or "").strip() for v in (vs or []) if v])
            or_groups.append([(c, merged[c]) for c in merged])
        else:
            for c, vs in bucket:
                for v in (vs or []):
                    v = (v or "").strip()
                    if not v:
                        continue
                    # normalize some obvious cases
                    if is_email(v):
                        v = v.lower()
                    eq_map.setdefault(c, []).append(v)

    # dedup, preserve order
    eq_filters: List[Tuple[str, List[str]]] = []
    for c, vals in eq_map.items():
        seen = set()
        uniq = []
        for v in vals:
            if v in seen:
                continue
            seen.add(v)
            uniq.append(v)
        eq_filters.append((c, uniq))
    return eq_filters, or_groups
