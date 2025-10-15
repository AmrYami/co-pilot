# -*- coding: utf-8 -*-
from __future__ import annotations
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import List, Dict, Tuple, Optional
from calendar import monthrange

# -----------------------------
# Datamodel للـ Intent الخاص بـ /dw/rate
# -----------------------------
@dataclass
class DateWindow:
    kind: str  # 'REQUEST' | 'OVERLAP' | 'END_ONLY'
    start: date
    end: date
    # عمود صريح اختاره المستخدم غير الافتراضى (اختيارى):
    explicit_col: Optional[str] = None

@dataclass
class RateIntent:
    table: str = "Contract"
    fts_groups: List[List[str]] = field(default_factory=list)
    eq_filters: Dict[str, List[str]] = field(default_factory=dict)
    neq_filters: Dict[str, List[str]] = field(default_factory=dict)
    contains: Dict[str, List[str]] = field(default_factory=dict)
    not_contains: Dict[str, List[str]] = field(default_factory=dict)
    empty_all: List[str] = field(default_factory=list)         # كلهم فاضى (AND)
    empty_any: List[str] = field(default_factory=list)         # أى واحد فاضى (OR)
    not_empty: List[str] = field(default_factory=list)
    order_by: Optional[str] = None
    top_n: Optional[int] = None
    top_by_col: Optional[str] = None
    date_window: Optional[DateWindow] = None

# -----------------------------
# Utilities
# -----------------------------
_AR_NUMS = {
    "واحد":1,"واحدة":1,"اثنين":2,"اتنين":2,"ثلاث":3,"ثلاثة":3,"اربع":4,"أربعة":4,
    "خمس":5,"خمسة":5,"ست":6,"ستة":6,"سبع":7,"سبعة":7,"ثمان":8,"ثمانية":8,"تسع":9,"تسعة":9,
    "عشر":10,"عشرة":10
}
def ar_to_int(tok: str) -> Optional[int]:
    return _AR_NUMS.get(tok.strip().lower())

def _today() -> date:
    return datetime.now().date()

def _start_of_month(d: date) -> date:
    return d.replace(day=1)

def _end_of_month(d: date) -> date:
    return d.replace(day=monthrange(d.year, d.month)[1])

def _start_of_quarter(d: date) -> date:
    m = ((d.month - 1)//3)*3 + 1
    return date(d.year, m, 1)

def _end_of_quarter(d: date) -> date:
    s = _start_of_quarter(d)
    e = _shift_months(s, 3) - timedelta(days=1)
    return e


def _shift_months(d: date, months: int) -> date:
    year = d.year + (d.month - 1 + months) // 12
    month = (d.month - 1 + months) % 12 + 1
    day = min(d.day, monthrange(year, month)[1])
    return date(year, month, day)


def _shift_years(d: date, years: int) -> date:
    try:
        return d.replace(year=d.year + years)
    except ValueError:
        # handle Feb 29
        return d.replace(month=2, day=28, year=d.year + years)

# -----------------------------
# Parsing لصيغ التواريخ (EN/AR الأساسية)
# -----------------------------
_re_num_range = re.compile(r"\b(last|next)\s+(\d+)\s+(day|days|week|weeks|month|months|year|years|quarter|quarters)\b", re.I)
_re_between   = re.compile(r"\bbetween\s+(\d{4}-\d{2}-\d{2})\s+(and|-)\s+(\d{4}-\d{2}-\d{2})\b", re.I)
_re_from      = re.compile(r"\bfrom\s+(\d{4}-\d{2}-\d{2})\b", re.I)
_re_until     = re.compile(r"\b(until|to)\s+(\d{4}-\d{2}-\d{2})\b", re.I)

def parse_date_phrase(text: str) -> Optional[Tuple[str, date, date]]:
    """
    يرجّع (label, start, end) لو قدر يفسّر العبارة الزمنية العامة
    label ممكن يكون: 'LAST_N_UNITS', 'NEXT_N_UNITS', 'THIS_MONTH', 'THIS_QUARTER', 'THIS_YEAR',
                     'LAST_QUARTER', 'NEXT_QUARTER', 'BETWEEN', 'FROM_TO'
    """
    s = text.strip().lower()

    # this/current
    if re.search(r"\b(this|current)\s+month\b", s) or "هذا الشهر" in s or "الشهر الحالي" in s:
        today = _today()
        return ("THIS_MONTH", _start_of_month(today), _end_of_month(today))
    if re.search(r"\b(this|current)\s+quarter\b", s) or "الربع الحالي" in s:
        today = _today()
        return ("THIS_QUARTER", _start_of_quarter(today), _end_of_quarter(today))
    if re.search(r"\b(this|current)\s+year\b", s) or "هذه السنة" in s or "السنة الحالية" in s:
        y = _today().year
        return ("THIS_YEAR", date(y,1,1), date(y,12,31))

    # last / next quarter (واحد or N quarters)
    if re.search(r"\blast\s+quarter\b", s) or "الربع الماضي" in s:
        tq = _shift_months(_start_of_quarter(_today()), -3)
        return ("LAST_QUARTER", tq, _end_of_quarter(tq))
    if re.search(r"\bnext\s+quarter\b", s) or "الربع القادم" in s or "الربع الجاي" in s:
        nq = _shift_months(_start_of_quarter(_today()), 3)
        return ("NEXT_QUARTER", nq, _end_of_quarter(nq))

    # last/next N units (EN)
    m = _re_num_range.search(s)
    if m:
        side, n, unit = m.group(1).lower(), int(m.group(2)), m.group(3).lower()
        today = _today()
        if unit.startswith("day"):
            delta = timedelta(days=n)
        elif unit.startswith("week"):
            delta = timedelta(weeks=n)
        elif unit.startswith("month"):
            # months/quarters بالـ relativedelta
            if side == "last":
                start = _shift_months(today, -n)
                return ("LAST_N_MONTHS", start, today)
            else:
                end = _shift_months(today, n)
                return ("NEXT_N_MONTHS", today, end)
        elif unit.startswith("year"):
            if side == "last":
                start = _shift_years(today, -n)
                return ("LAST_N_YEARS", start, today)
            else:
                end = _shift_years(today, n)
                return ("NEXT_N_YEARS", today, end)
        elif unit.startswith("quarter"):
            if side == "last":
                start = _shift_months(today, -3*n)
                return ("LAST_N_QUARTERS", start, today)
            else:
                end = _shift_months(today, 3*n)
                return ("NEXT_N_QUARTERS", today, end)

        if side == "last":
            return ("LAST_N_DW", today - delta, today)
        else:
            return ("NEXT_N_DW", today, today + delta)

    # between / from / until
    m = _re_between.search(s)
    if m:
        d1 = date.fromisoformat(m.group(1))
        d2 = date.fromisoformat(m.group(4))
        if d2 < d1:
            d1, d2 = d2, d1
        return ("BETWEEN", d1, d2)
    m = _re_from.search(s)
    if m:
        d1 = date.fromisoformat(m.group(1))
        return ("FROM_TO", d1, _today())
    m = _re_until.search(s)
    if m:
        d2 = date.fromisoformat(m.group(2))
        # نافذة من قديم الأزل لحد d2
        return ("FROM_TO", date(1970,1,1), d2)

    # عربى: "الشهر الماضي/اللى فات" ، "الأسبوع القادم" ، "السنة الماضية"…
    ar = s
    if "الشهر الماضي" in ar or "الشهر اللى فات" in ar:
        t = _today()
        last_m = _shift_months(_start_of_month(t), -1)
        return ("LAST_MONTH", last_m, _end_of_month(last_m))
    if "الشهر القادم" in ar or "الشهر الجاي" in ar:
        t = _today()
        next_m = _shift_months(_start_of_month(t), 1)
        return ("NEXT_MONTH", next_m, _end_of_month(next_m))
    if "الأسبوع الماضي" in ar or "الاسبوع الماضي" in ar:
        t = _today()
        return ("LAST_WEEK", t - timedelta(days=7), t)
    if "الأسبوع القادم" in ar or "الاسبوع القادم" in ar or "الأسبوع الجاي" in ar:
        t = _today()
        return ("NEXT_WEEK", t, t + timedelta(days=7))

    return None

# -----------------------------
# تحديد نوع النافذة حسب العبارة
# -----------------------------
def decide_window_kind(full_comment: str) -> str:
    s = full_comment.lower()
    if "requested" in s or "طلب" in s or "تم إنشاؤه" in s:
        return "REQUEST"
    if "expir" in s or "سينتهي" in s or "منتهي" in s or "انتهاء" in s:
        return "END_ONLY"
    # الافتراض: active/overlap
    return "OVERLAP"

# -----------------------------
# بناء شروط التاريخ + الـ binds
# -----------------------------
def build_date_predicate(intent: RateIntent,
                         comment: str,
                         settings: dict) -> Tuple[Optional[str], Dict[str, object]]:
    # لو intent فيه نافذة محددة مسبقًا استخدمها مباشرة
    if intent.date_window:
        dw = intent.date_window
        binds = {"date_start": dw.start, "date_end": dw.end}
        kind = (dw.kind or "OVERLAP").upper()
        col = dw.explicit_col
        if kind == "REQUEST":
            col = col or "REQUEST_DATE"
            return f"({col} BETWEEN :date_start AND :date_end)", binds
        if kind == "END_ONLY":
            col = col or "END_DATE"
            return f"({col} BETWEEN :date_start AND :date_end)", binds
        left = "START_DATE"
        right = "END_DATE"
        return f"({left} <= :date_end AND {right} >= :date_start)", binds
    # حاول استخراج نافذة زمنية من النص
    parsed = parse_date_phrase(comment)
    if not parsed:
        return None, {}
    label, ds, de = parsed
    kind = decide_window_kind(comment)

    # لو المستخدم سمّى عمود صريح، ممكن تمرره فى intent.date_window.explicit_col قبل النداء
    dw = DateWindow(kind=kind, start=ds, end=de)
    intent.date_window = dw

    binds = {"date_start": ds, "date_end": de}

    # اختيار العمود/الأعمدة
    if kind == "REQUEST":
        col = "REQUEST_DATE"
        if intent.date_window.explicit_col:
            col = intent.date_window.explicit_col
        return f"({col} BETWEEN :date_start AND :date_end)", binds

    if kind == "END_ONLY":
        col = "END_DATE"
        if intent.date_window.explicit_col:
            col = intent.date_window.explicit_col
        return f"({col} BETWEEN :date_start AND :date_end)", binds

    # OVERLAP صارم على START/END
    left  = "START_DATE"
    right = "END_DATE"
    pred = f"({left} <= :date_end AND {right} >= :date_start)"
    return pred, binds

# -----------------------------
# توسيع aliases (DEPARTMENT/STAKEHOLDER) من settings
# -----------------------------
def expand_alias_columns(col: str, settings: dict) -> List[str]:
    aliases = settings.get("DW_EQ_ALIAS_COLUMNS") or {}
    if col.upper() in aliases:
        return aliases[col.upper()]
    return [col]

# -----------------------------
# بناء WHERE عام من intent
# -----------------------------
def build_rate_sql(intent: RateIntent, settings: dict) -> Tuple[str, Dict[str, object], Dict]:
    table = settings.get("DW_CONTRACT_TABLE", "Contract")
    where_clauses: List[str] = []
    binds: Dict[str, object] = {}
    bseq = {"eq":0,"neq":0,"like":0,"nlike":0,"fts":0}

    # 1) date window (لو موجودة فى الـ comment)
    dt_pred, dt_binds = build_date_predicate(intent, settings.get("_raw_comment",""), settings)
    if dt_pred:
        where_clauses.append(dt_pred)
        binds.update(dt_binds)

    # 2) EQ / NEQ
    def add_in_list(op: str, col: str, vals: List[str]):
        cols = expand_alias_columns(col, settings)
        inp = []
        for c in cols:
            bind_names = []
            for v in vals:
                k = f"{'eq' if op=='=' else 'neq'}_{bseq['eq' if op=='=' else 'neq']}"
                bseq['eq' if op=='=' else 'neq'] += 1
                binds[k] = str(v).upper()
                bind_names.append(f"UPPER(:{k})")
            inp.append(f"(UPPER(TRIM({c})) {'IN' if op=='=' else 'NOT IN'} ({', '.join(bind_names)}))")
        # لو alias فيه أكتر من عمود: OR بينهم
        return "(" + (" OR ".join(inp)) + ")"

    for col, vals in intent.eq_filters.items():
        where_clauses.append(add_in_list("=", col, vals))
    for col, vals in intent.neq_filters.items():
        where_clauses.append(add_in_list("<>", col, vals))

    # 3) contains / not_contains
    def add_like_list(neg: bool, col: str, vals: List[str]):
        frag = []
        for v in vals:
            k = f"{'nlike' if neg else 'like'}_{bseq['nlike' if neg else 'like']}"
            bseq['nlike' if neg else 'like'] += 1
            binds[k] = f"%{v}%".upper()
            frag.append(f"UPPER(NVL({col},'')) {'NOT LIKE' if neg else 'LIKE'} UPPER(:{k})")
        # AND بين القيم (كل كلمة لازم ما تظهرش/تظهر)
        return "(" + (" AND ".join(frag)) + ")"
    for col, vals in intent.contains.items():
        where_clauses.append(add_like_list(False, col, vals))
    for col, vals in intent.not_contains.items():
        where_clauses.append(add_like_list(True, col, vals))

    # 4) empty / empty_any / not_empty
    if intent.empty_all:
        where_clauses += [f"(TRIM(NVL({c},''))='')" for c in intent.empty_all]
    if intent.empty_any:
        ors = [f"(TRIM(NVL({c},''))='')" for c in intent.empty_any]
        where_clauses.append("(" + " OR ".join(ors) + ")")
    if intent.not_empty:
        where_clauses += [f"(TRIM(NVL({c},''))<>'')" for c in intent.not_empty]

    # 5) FTS (OR على المجموعات، وكل مجموعة AND على توكنزها؟ حسب سلوكك)
    # هنستخدم UPPER(NVL(col,'')) LIKE لكل الأعمدة المحددة فى DW_FTS_COLUMNS
    fts_cols = (settings.get("DW_FTS_COLUMNS") or {}).get(intent.table, []) or \
               (settings.get("DW_FTS_COLUMNS") or {}).get("*", [])
    for grp in intent.fts_groups:
        # group = token list → ( (col1 like tok1 OR col2 like tok1 ... ) OR (.. tok2 ..) )
        tok_ors = []
        for tok in grp:
            k = f"fts_{bseq['fts']}"
            bseq['fts'] += 1
            binds[k] = f"%{tok}%".upper()
            col_ors = [f"UPPER(NVL({c},'')) LIKE UPPER(:{k})" for c in fts_cols]
            tok_ors.append("(" + " OR ".join(col_ors) + ")")
        where_clauses.append("(" + " OR ".join(tok_ors) + ")")

    # تجميع WHERE
    where_sql = ""
    if where_clauses:
        where_sql = "WHERE " + " AND ".join(where_clauses)

    # 6) ORDER / TOP
    order_sql = ""
    if intent.order_by:
        order_sql = f" ORDER BY {intent.order_by}"
    if intent.top_n and intent.top_by_col:
        # تأكد ان order موجود
        if not order_sql:
            order_sql = f" ORDER BY {intent.top_by_col} DESC"
        order_sql += f" FETCH FIRST {int(intent.top_n)} ROWS ONLY"

    sql = f'SELECT * FROM "{table}"\n{where_sql}{order_sql}'
    debug = {"where": where_clauses, "binds": binds, "fts_cols_count": len(fts_cols)}
    return sql, binds, debug
