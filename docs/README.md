*** Begin Patch
*** Add File: README.md
#!/usr/bin/env markdown
# co-pilot

DW SQL Copilot that converts natural-language questions into Oracle SQL over the DocuWare `Contract` table, with DB-backed settings, FTS/EQ rules, `/dw/rate` instant fixes, and golden test gating.

## Start here
- **Welcome** → [`docs/WELCOME.md`](docs/WELCOME.md)
- **Index (all docs)** → [`docs/INDEX.md`](docs/INDEX.md)

## Quick TL;DR
- Settings live in DB (namespace `dw::common`) via `/admin/settings/bulk`.
- FTS uses `DW_FTS_ENGINE` (`like`)  `DW_FTS_COLUMNS`.
- Equality only for `DW_EXPLICIT_FILTER_COLUMNS`; `REQUEST_TYPE` has synonyms.
- `/dw/answer` builds SQL; `/dw/rate` applies instant fixes (`fts:`, `eq:`, `group_by:`, `order_by:`).
- Golden must pass before merge.

## Runbook (short)
```bash
python scripts/export_context.py --out docs/state
curl -X POST http://localhost:5000/admin/run_golden -H 'Content-Type: application/json' -d '{}'
```

## Notes
- Code comments are in English only.
- No hardcoded settings—always read from DB.

*** End Patch
