from __future__ import annotations

from .models import NLIntent


def build_explain(intent: NLIntent) -> str:
    parts = []
    if intent.explicit_dates:
        start = intent.explicit_dates.get("start")
        end = intent.explicit_dates.get("end")
        if start and end:
            parts.append(f"فسرتُ النافذة الزمنية: {start} → {end}.")
    if intent.date_column == "OVERLAP":
        parts.append("تعريف ‘contracts’ = ناشطة ضمن النافذة (تداخل START_DATE/END_DATE).")
    elif intent.date_column:
        parts.append(f"استخدمت العمود الزمني: {intent.date_column}.")
    if intent.group_by:
        parts.append(f"تجميع حسب: {intent.group_by}.")
    if intent.agg:
        parts.append(f"تجميع ({intent.agg}).")
    if intent.sort_by:
        parts.append("رتّبت حسب القيمة" + (" تنازلياً." if intent.sort_desc else " تصاعدياً."))
    if intent.full_text_search:
        parts.append("فعّلت البحث النصي عبر الأعمدة المُهيّأة.")
    if intent.wants_all_columns and not intent.group_by and not intent.agg:
        parts.append("عرضت جميع الأعمدة لأن السؤال لم يطلب أعمدة محددة.")
    return " ".join(parts)
