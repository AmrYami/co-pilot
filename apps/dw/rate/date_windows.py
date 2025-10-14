# -*- coding: utf-8 -*-
from __future__ import annotations
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Optional, Literal, Dict, Tuple

try:
    # اختياري: لو متاح بيحسّن parsing للتواريخ الحرة
    import dateparser  # type: ignore
except Exception:
    dateparser = None


@dataclass
class DateWindow:
    kind: Literal["OVERLAP", "REQUEST", "END_ONLY"] = "OVERLAP"
    col: Optional[str] = None  # REQUEST_DATE | START_DATE | END_DATE (لو مذكورة صراحة)
    start: Optional[date] = None
    end: Optional[date] = None
    detected: Optional[str] = None  # النص المكتشف (لـ debug)
    suggested_order_by: Optional[str] = None  # ORDER BY مناسب للنافذة


AR_NUMS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")


def _norm(s: str) -> str:
    s = s.translate(AR_NUMS)
    s = re.sub(r"\s+", " ", s.strip())
    return s.lower()


# ---- أنماط اللغة ----
RE_EXPLICIT_COL = re.compile(r"\b(REQUEST_DATE|START_DATE|END_DATE)\b", re.I)
RE_REQUESTED = re.compile(r"\b(requested|request date|created|طلب|تم الطلب)\b")
RE_EXPIRING = re.compile(r"\b(expiring|expires|will expire|انتهاء|ينتهي|تنتهي|انقضاء)\b")

RE_LAST_NEXT_QUARTER = re.compile(r"\b(?:last|next|آخر|القادم|التالي)\s+(?:quarter|ربع)\b")
RE_LAST_NEXT_N = re.compile(
    r"\b(?P<dir>last|next|آخر|الماضي|السابق|القادم|التالي)\s+"
    r"(?P<n>\d+)\s*"
    r"(?P<unit>days?|weeks?|months?|years?|يوم|أيام|اسبوع|أسبوع|أسابيع|شهر|أشهر|شهور|سنة|سنين|سنوات)\b"
)

RE_BETWEEN = re.compile(
    r"(?:between|بين)\s+(?P<d1>[^;,\n]+?)\s*"
    r"(?:and|و|to|حتى|(?<=\s)-+(?=\s)|–+|\.\.)\s*(?P<d2>[^;,\n]+)",
    re.I,
)
RE_DOTS_RANGE = re.compile(
    r"(?P<d1>\d{1,4}[^A-Za-z0-9]\d{1,2}[^A-Za-z0-9]\d{2,4})\s*\.\.\s*(?P<d2>.+)"
)
RE_FROM = re.compile(r"(?:from|من)\s+(?P<d1>\d{1,4}[^A-Za-z0-9]\d{1,2}[^A-Za-z0-9]\d{2,4})", re.I)
RE_UNTIL = re.compile(r"(?:until|to|حتى|الى|إلى)\s+(?P<d2>\d{1,4}[^A-Za-z0-9]\d{1,2}[^A-Za-z0-9]\d{2,4})", re.I)


# ---- توابع مساعدة ----
def _parse_date_any(s: str) -> Optional[date]:
    s = _norm(s)
    # جرّب dateparser لو موجود
    if dateparser:
        dt = dateparser.parse(s, settings={"PREFER_DAY_OF_MONTH": "first", "DATE_ORDER": "DMY"})
        if dt:
            return dt.date()
    # fallback: فورمات شائعة
    fmts = [
        "%Y-%m-%d",
        "%d-%m-%Y",
        "%m-%d-%Y",
        "%Y/%m/%d",
        "%d/%m/%Y",
        "%m/%d/%Y",
        "%Y.%m.%d",
        "%d.%m.%Y",
        "%m.%d.%Y",
        "%d %b %Y",
        "%d %B %Y",
        "%b %d %Y",
        "%B %d %Y",
    ]
    for f in fmts:
        try:
            return datetime.strptime(s, f).date()
        except Exception:
            pass
    return None


def _quarter_bounds(d: date) -> Tuple[date, date]:
    q = (d.month - 1) // 3 + 1
    start_month = 3 * (q - 1) + 1
    q_start = date(d.year, start_month, 1)
    if start_month == 10:
        q_end = date(d.year, 12, 31)
    else:
        # أول يوم في الشهر التالي للربع - يوم
        if start_month in (1, 4, 7):
            next_start = date(d.year, start_month + 3, 1)
        else:
            next_start = date(d.year + 1, 1, 1)
        q_end = next_start - timedelta(days=1)
    return q_start, q_end


def _add_months(d: date, months: int) -> date:
    # بدون تبعية خارجية
    y = d.year + (d.month - 1 + months) // 12
    m = (d.month - 1 + months) % 12 + 1
    day = min(
        d.day,
        [
            31,
            29 if y % 4 == 0 and (y % 100 != 0 or y % 400 == 0) else 28,
            31,
            30,
            31,
            30,
            31,
            31,
            30,
            31,
            30,
            31,
        ][m - 1],
    )
    return date(y, m, day)


def _start_of_month(d: date) -> date:
    return date(d.year, d.month, 1)


def _end_of_month(d: date) -> date:
    first_next = _add_months(_start_of_month(d), 1)
    return first_next - timedelta(days=1)


# ---- المنطق الرئيسي ----
def detect_date_window(text: str, *, today: Optional[date] = None) -> Optional[DateWindow]:
    t = _norm(text)
    today = today or date.today()
    win = DateWindow(kind="OVERLAP", suggested_order_by="REQUEST_DATE DESC")

    # 1) Explicit column hint
    mcol = RE_EXPLICIT_COL.search(text)
    if mcol:
        win.col = mcol.group(1).upper()

    # 2) Kind detection (requested / expiring)
    if RE_REQUESTED.search(t) or (win.col == "REQUEST_DATE"):
        win.kind = "REQUEST"
        win.suggested_order_by = "REQUEST_DATE DESC"
    elif RE_EXPIRING.search(t) or (win.col == "END_DATE"):
        win.kind = "END_ONLY"
        win.suggested_order_by = "END_DATE ASC"
    else:
        win.kind = "OVERLAP"
        # تظل order_by على REQUEST_DATE DESC كافتراضي

    # 3) Ranges: between / .. / from / until
    m = RE_BETWEEN.search(t) or RE_DOTS_RANGE.search(t)
    if m:
        d1 = _parse_date_any(m.group("d1"))
        d2 = _parse_date_any(m.group("d2"))
        if d1 and d2 and d1 <= d2:
            win.start, win.end = d1, d2
            win.detected = f"between:{d1}..{d2}"
            return win

    mf = RE_FROM.search(t)
    mu = RE_UNTIL.search(t)
    if mf or mu:
        d1 = _parse_date_any(mf.group("d1")) if mf else date(1, 1, 1)
        d2 = _parse_date_any(mu.group("d2")) if mu else today
        if d1 and d2 and d1 <= d2:
            win.start, win.end = d1, d2
            win.detected = f"from:{d1}-to:{d2}"
            return win

    # 4) last/next quarter
    mq = RE_LAST_NEXT_QUARTER.search(t)
    if mq:
        # احسب ربع "حالي" ثم حرّك ±1
        q_start, q_end = _quarter_bounds(today)
        if "last" in mq.group(0) or "آخر" in mq.group(0) or "السابق" in mq.group(0) or "الماضي" in mq.group(0):
            # الربع الماضي
            base = q_start.replace(day=1)
            prev_q_end = base - timedelta(days=1)
            prev_q_start, prev_q_end2 = _quarter_bounds(prev_q_end)
            win.start, win.end = prev_q_start, prev_q_end2
            win.detected = "last_quarter"
        else:
            # الربع القادم
            next_q_start = (q_end + timedelta(days=1)).replace(day=1)
            # q_end بالفعل نهاية الربع الحالي؛ بعده يبدأ الشهر الجديد…
            nqs, nqe = _quarter_bounds(next_q_start)
            win.start, win.end = nqs, nqe
            win.detected = "next_quarter"
        return win

    # 5) last|next N units
    mn = RE_LAST_NEXT_N.search(t)
    if mn:
        raw = mn.groupdict()
        n = int(raw["n"])
        unit = raw["unit"]
        is_last = raw["dir"] in ("last", "آخر", "الماضي", "السابق")
        if unit.startswith(("day", "يوم", "أيا")):
            delta = timedelta(days=n)
        elif unit.startswith(("week", "اسبوع", "أسبوع", "أسابي")):
            delta = timedelta(days=7 * n)
        elif unit.startswith(("month", "شهر", "أشه", "شهو")):
            # نحسب بالنشور: من اليوم ← ± n شهور
            if is_last:
                start = _add_months(today, -n)
                win.start, win.end = start, today
            else:
                end = _add_months(today, +n)
                win.start, win.end = today, end
            win.detected = f"{'last' if is_last else 'next'}_{n}_months"
            return win
        elif unit.startswith(("year", "سنة", "سنين", "سنوا")):
            if is_last:
                start = date(today.year - n, today.month, today.day)
                win.start, win.end = start, today
            else:
                end = date(today.year + n, today.month, today.day)
                win.start, win.end = today, end
            win.detected = f"{'last' if is_last else 'next'}_{n}_years"
            return win
        else:
            delta = timedelta(days=0)

        if is_last:
            win.start, win.end = today - delta, today
        else:
            win.start, win.end = today, today + delta
        win.detected = f"{'last' if is_last else 'next'}_{n}_{unit}"
        return win

    # 6) last month / next month / last week … إلخ (بدون N)
    if "last month" in t or "الشهر الماضي" in t:
        first_this = _start_of_month(today)
        win.start = _start_of_month(_add_months(first_this, -1))
        win.end = first_this - timedelta(days=1)
        win.detected = "last_month"
        return win
    if "next month" in t or "الشهر القادم" in t or "الشهر التالي" in t:
        first_next = _start_of_month(_add_months(today, +1))
        win.start = first_next
        win.end = _end_of_month(first_next)
        win.detected = "next_month"
        return win
    if "last week" in t or "الأسبوع الماضي" in t:
        win.start = today - timedelta(days=7)
        win.end = today
        win.detected = "last_week"
        return win
    if "next week" in t or "الأسبوع القادم" in t or "الأسبوع التالي" in t:
        win.start = today
        win.end = today + timedelta(days=7)
        win.detected = "next_week"
        return win

    # لم يُكتشف شيء صريح—نرجّع None ليكمل الـ rate بدون نافذة تاريخ
    return None


def compile_date_sql(
    win: DateWindow, *, overlap_require_both: bool = True, overlap_strict: bool = False
) -> Tuple[str, Dict[str, object], str]:
    """
    يُحوّل DateWindow إلى:
      - sql_fragment (بدون WHERE)
      - binds {'date_start': date, 'date_end': date}
      - suggested_order_by
    """
    if not (win.start and win.end):
        # ما فيش حدود → لا شيء
        return "", {}, win.suggested_order_by or "REQUEST_DATE DESC"

    binds = {"date_start": win.start, "date_end": win.end}

    if win.col:
        frag = f"{win.col} BETWEEN :date_start AND :date_end"
    elif win.kind == "REQUEST":
        frag = "REQUEST_DATE BETWEEN :date_start AND :date_end"
    elif win.kind == "END_ONLY":
        frag = "END_DATE BETWEEN :date_start AND :date_end"
    else:
        # OVERLAP
        core = "(START_DATE <= :date_end AND END_DATE >= :date_start)"
        if overlap_strict and overlap_require_both:
            frag = f"{core} AND START_DATE IS NOT NULL AND END_DATE IS NOT NULL"
        else:
            frag = core

    return frag, binds, win.suggested_order_by or "REQUEST_DATE DESC"

