#!/usr/bin/env markdown
# ADR-0001: DB-Backed Settings

**Decision**: جميع الإعدادات تُقرأ من DB (namespace: `dw::common`) بدل ملفات ثابتة.  
**Why**: سهولة التبديل/الحوكمة/المراجعة.  
**How**: ملف `apps/dw/settings.py` يوحّد الوصول.
