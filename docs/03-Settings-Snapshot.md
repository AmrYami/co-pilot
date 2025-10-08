# سنابشوت الإعدادات (Settings Snapshot)

الإعدادات تُدار من `/admin/settings/bulk` (namespace: `dw::common`).  
لتصدير نسخة JSON إلى `docs/state/settings_export.json`:

```bash
MEMORY_DB_URL=postgresql+psycopg2://... \
python scripts/export_context.py --out docs/state
```

**مفاتيح رئيسية:**
- `DW_FTS_ENGINE` = `"like"`
- `DW_FTS_COLUMNS` = `{ "Contract": [...], "*": [...] }`
- `DW_EXPLICIT_FILTER_COLUMNS` = `[ENTITY, ENTITY_NO, REQUEST_TYPE, REQUESTER, CONTRACT_ID, REPRESENTATIVE_EMAIL, ...]`
- `DW_ENUM_SYNONYMS` = مرادفات `Contract.REQUEST_TYPE`
- `DW_CONTRACT_TABLE` = `"Contract"`, `DW_DATE_COLUMN` = `"REQUEST_DATE"`
