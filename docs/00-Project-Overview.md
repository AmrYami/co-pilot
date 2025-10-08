# co-pilot — نظرة عامة (Overview)

هذا المشروع عبارة عن **DW SQL Copilot** يحوّل الأسئلة الطبيعية إلى SQL على جدول العقود (DocuWare).

**TL;DR**
- الإعدادات مركزية في قاعدة البيانات (namespace: `dw::common`) عبر `/admin/settings/bulk`.
- البحث النصّي FTS يعتمد على `DW_FTS_ENGINE` (`like` افتراضيًا) وأعمدة `DW_FTS_COLUMNS`.
- فلاتر المساواة (Equality) مسموحة فقط لأعمدة `DW_EXPLICIT_FILTER_COLUMNS`. قيم `REQUEST_TYPE` لها مرادفات في `DW_ENUM_SYNONYMS`.
- `/dw/answer` يبني SQL وينفّذ؛ `/dw/rate` يطبّق تصحيحات فورية من تعليق المستخدم (`fts:`, `eq:`, `group_by:`, `order_by:`).
- Golden tests لازم تعدّي قبل أي دمج.

## روابط مفيدة
- بنية المشروع: `docs/01-Architecture.md`
- واجهات وخدمات: `docs/04-Routes-and-APIs.md`
- إعدادات وسنابشوت: `docs/03-Settings-Snapshot.md`
- التشغيل والتجارب: `docs/08-Runbook.md`
- خطة الطريق: `docs/09-Roadmap-and-Backlog.md`
