# دليل التشغيل (Runbook)

## متطلبات
- Python 3.12+
- Oracle client (oracledb)
- PostgreSQL (MEMORY_DB_URL) للتعلّم/الإدارة

## .env (مثال مختصر)
```
MEMORY_DB_URL=postgresql+psycopg2://postgres:***@localhost/copilot_mem_dev
APP_DB_URL=oracle+oracledb://user:pass@localhost:1521/?service_name=FREEPDB1
DW_FTS_ENGINE=like
```

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
