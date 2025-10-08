#!/usr/bin/env markdown
# الاختبارات و Golden

## تشغيل
- `POST /admin/run_golden` → يعيد تقريرًا (passed/failed مع الأسباب).

## YAML (أفكار أساسية)
- **Presence**: `must_contain`, `must_not_contain`
- **Order**: `require_order_by`, `require_order_dir`
- **Grouping**: `require_group_by`
- **Explain**: `require_explain_contains`

## وسوم التواريخ
- استخدم وسوم ثابتة مع Loader (أو استعمل قيم تواريخ مباشرة لتجنّب أخطاء YAML).
