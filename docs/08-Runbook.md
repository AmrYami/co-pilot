#!/usr/bin/env markdown
# دليل التشغيل (Runbook)

## متطلبات
- Python 3.12+
- Oracle client (oracledb)
- PostgreSQL (MEMORY_DB_URL) للتعلّم/الإدارة

## .env
ضع ملف `.env` الخاص بك في جذر المشروع (الذي رفعته) — أو استخدم المتغيرات بيئيًا.

## Populate settings
استخدم `/admin/settings/bulk` بالقيم الموجودة في `docs/03-Settings-Snapshot.md`.

## تصدير الحالة إلى docs/state
```bash
python scripts/export_context.py --out docs/state
```

## Golden
```bash
curl -X POST http://localhost:5000/admin/run_golden -H 'Content-Type: application/json' -d '{}'
```
