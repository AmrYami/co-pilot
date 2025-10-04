import re
from typing import Dict, List, Tuple, Any


def _canon(s: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", s.upper())


def pick_column(name_text: str, explicit_columns: List[str]) -> str | None:
    """
    يحاول يطابق النص مع اسم عمود مُعرّف في DW_EXPLICIT_FILTER_COLUMNS
    التطبيع بإزالة المسافات والشرط والـ underscore.
    """
    canon_target = _canon(name_text)
    best = None
    for col in explicit_columns or []:
        if _canon(col) == canon_target:
            return col
        # تمايز بسيط: لو النص جزء واضح من العمود
        if _canon(col).endswith(canon_target) or canon_target.endswith(_canon(col)):
            best = best or col
    return best


_EQ_PATTERNS = [
    r"(?i)\bwhere\s+([A-Z0-9_ \-]+?)\s*(?:=|==|equals|is)\s*'([^']+)'",
    r"(?i)\b([A-Z0-9_ \-]+?)\s*(?:=|==|equals|is)\s*'([^']+)'",
    r"(?i)\b([A-Z0-9_ \-]+?)\s*(?:=|==|equals|is)\s*([A-Z0-9_\-\.]+)"
]


def extract_eq_filters(question: str,
                       explicit_columns: List[str],
                       column_synonyms: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    يكتشف أنماط: COLUMN = VALUE من نص السؤال.
    - يرجّع قائمة فلاتر: [{"col":"REQUEST_TYPE","val":"Renewal", "ci":True, "trim":True, "synonyms":[...]}]
    - لو لقى مرادفات للعمود المحدد (مثل Contract.REQUEST_TYPE) بيضيفها ضمن الـ payload.
    """
    eqs: List[Dict[str, Any]] = []
    q_norm = question or ""
    for pat in _EQ_PATTERNS:
        for m in re.finditer(pat, q_norm):
            left, right = m.group(1), m.group(2)
            col = pick_column(left, explicit_columns)
            if not col:
                continue
            val = right.strip().strip('"').strip()
            payload = {"col": col, "val": val, "ci": True, "trim": True, "op": "eq"}
            # لو عندنا مرادفات معرفّة للعمود ده
            # ملاحظة: map المفروض تكون: {"Contract.REQUEST_TYPE": {...}}
            for fq_col, synmap in (column_synonyms or {}).items():
                # fq_col ممكن يكون "Contract.REQUEST_TYPE" أو "*."<col>
                colname = fq_col.split(".")[-1]
                if _canon(colname) == _canon(col):
                    payload["synonyms"] = synmap
                    break
            eqs.append(payload)
    return eqs


def _collect_fts_tokens(question: str) -> Tuple[List[str], str]:
    """
    يستخرج كلمات البحث من السؤال.
    - يدعم صيغة: "has it or home care", "has it and x"
    - الافتراضي OR.
    - لو "and" موجودة بوضوح → AND.
    """
    q = (question or "").lower()
    # التوكنز الخام (نحذف كلمات ربط غير مهمة)
    # مثال سريع: اسحب ما بعد "has" إن وجدت
    tokens_area = q
    m = re.search(r"\bhas\s+(.+)", q)
    if m:
        tokens_area = m.group(1)

    # نفصل على or/and، لكن نحتاج نعرف هل فيه and صريحة
    has_and = " and " in tokens_area
    # نفصل على both and/or وننظّف
    rough = re.split(r"\s+or\s+|\s+and\s+", tokens_area)
    toks = []
    for t in rough:
        t = t.strip(" ,.;:/\\|()[]{}\"'").strip()
        if t:
            toks.append(t)
    mode = "AND" if has_and else "OR"
    return toks, mode


def build_fts_predicates(question: str,
                         fts_columns: List[str],
                         *,
                         bind_prefix: str = "fts") -> Tuple[str, Dict[str, Any]]:
    """
    يبني WHERE لبحث نصّي بسيط على قائمة الأعمدة.
    يرجّع: (sql_predicate, binds)
    - tokens تُوزّع على الأعمدة بـ OR، وبين التوكنز حسب وجود and/or.
    """
    tokens, mode = _collect_fts_tokens(question)
    cols = [c for c in fts_columns or [] if c]  # احرص إن القائمة مش فاضية

    if not tokens or not cols:
        return "", {}

    binds: Dict[str, Any] = {}
    groups = []
    for i, tok in enumerate(tokens):
        bind_name = f"{bind_prefix}_{i}"
        binds[bind_name] = f"%{tok}%"
        per_token = []
        for c in cols:
            per_token.append(f"UPPER(TRIM({c})) LIKE UPPER(:{bind_name})")
        groups.append("(" + " OR ".join(per_token) + ")")
    joiner = f" {mode} "
    pred = "(" + joiner.join(groups) + ")"
    return pred, binds


def synonyms_to_like_clauses(
    col: str,
    target_val: str,
    synmap: Dict[str, Any] | None,
    *,
    bind_prefix: str | None = None,
) -> Tuple[str, Dict[str, Any]]:
    """
    يحوّل مرادفات القيمة لنصوص LIKE/=/IS NULL حسب التعريف.
    synmap مثال: 
      {
        "renewal": {
          "equals": ["Renewal","Renew","Renew Contract","Renewed","extension"],
          "prefix": ["Renew","Extens"],
          "contains": []
        },
        "null": { "equals": ["NULL","N/A","NA","-"], "prefix":[], "contains":[] }
      }
    """
    safe_col = re.sub(r"[^A-Za-z0-9_]", "_", str(col).strip()) or "col"
    prefix = bind_prefix or f"eq_{safe_col.lower()}"

    if not synmap:
        bind_name = f"{prefix}_eq"
        return (
            "UPPER(TRIM({col})) = UPPER(:{bind})".format(col=col, bind=bind_name),
            {bind_name: target_val},
        )

    key_hit = None
    tv_up = target_val.strip().lower()
    for key, spec in synmap.items():
        eqs = [e.lower() for e in spec.get("equals", [])]
        prefs = [p.lower() for p in spec.get("prefix", [])]
        conts = [c.lower() for c in spec.get("contains", [])]
        if tv_up in eqs:
            key_hit = key
            break
        if any(tv_up.startswith(p) for p in prefs):
            key_hit = key
            break
        if any(tv_up in c for c in conts):
            key_hit = key
            break

    if not key_hit:
        bind_name = f"{prefix}_like"
        return (
            "UPPER(TRIM({col})) LIKE UPPER(:{bind})".format(col=col, bind=bind_name),
            {bind_name: f"%{target_val}%"},
        )

    spec = synmap[key_hit]
    clauses = []
    binds: Dict[str, Any] = {}
    bi = 0

    def _next(name: str) -> str:
        nonlocal bi
        token = f"{prefix}_{name}_{bi}"
        bi += 1
        return token

    if key_hit.lower() == "null":
        null_eqs = spec.get("equals", [])
        null_list = [n for n in null_eqs if n.upper() not in ("NULL",)]
        null_or = []
        if null_list:
            placeholders = []
            for s in null_list:
                name = _next("eq")
                binds[name] = s
                placeholders.append(f"UPPER(TRIM({col})) = UPPER(:{name})")
            null_or.append("(" + " OR ".join(placeholders) + ")")
        null_or.append(f"{col} IS NULL")
        null_or.append(f"TRIM({col}) = ''")
        clauses.append("(" + " OR ".join(null_or) + ")")
    else:
        for s in spec.get("equals", []):
            if not s:
                continue
            name = _next("eq")
            binds[name] = s
            clauses.append(f"UPPER(TRIM({col})) = UPPER(:{name})")
        for p in spec.get("prefix", []):
            if not p:
                continue
            name = _next("pre")
            binds[name] = f"{p}%"
            clauses.append(f"UPPER(TRIM({col})) LIKE UPPER(:{name})")
        for c in spec.get("contains", []):
            if not c:
                continue
            name = _next("con")
            binds[name] = f"%{c}%"
            clauses.append(f"UPPER(TRIM({col})) LIKE UPPER(:{name})")

    return "(" + " OR ".join(clauses) + ")", binds


def collect_fts_tokens(question: str) -> Tuple[List[str], str]:
    return _collect_fts_tokens(question)


__all__ = [
    "pick_column",
    "extract_eq_filters",
    "build_fts_predicates",
    "synonyms_to_like_clauses",
    "collect_fts_tokens",
]
