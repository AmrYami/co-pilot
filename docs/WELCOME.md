#!/usr/bin/env markdown
# Welcome to co-pilot (DW SQL Copilot)

**TL;DR**
- Settings are DB-backed (namespace: `dw::common`) via `/admin/settings/bulk`.
- FTS uses `DW_FTS_ENGINE` (`like`) and columns from `DW_FTS_COLUMNS`.
- Equality filters only for `DW_EXPLICIT_FILTER_COLUMNS`; `REQUEST_TYPE` has synonyms in `DW_ENUM_SYNONYMS`.
- `/dw/answer` builds SQL; `/dw/rate` applies instant fixes (`fts:`, `eq:`, `group_by:`, `order_by:`).
- Golden tests must pass before merging.

**How to continue**
1. Read `docs/00-Project-Overview.md` then `01-Architecture.md`.
2. Export state: `python scripts/export_context.py --out docs/state`
3. Run `/admin/run_golden` and fix any failing asserts.
4. Try `/dw/answer` and `/dw/rate` using patterns in `docs/05-Domain-DW.md`.

**Rate comment grammar (quick)**
```
fts: tok1 | tok2
eq:  COLUMN = VALUE (ci, trim)
group_by: COL1, COL2
order_by: COL asc|desc
top:  N by COL   # or bottom: N by COL
```

---

## Structure quick guide
- **core/**: project-agnostic reusable building blocks (settings infra, DB/session, logging, sql utils, validators, generic explain/rate). No domain-specific code here.  
- **apps/<app>/**: app/domain-specific logic (e.g., `apps/dw` for DocuWare).  
- **Table-scoped**: put each table logic under its own module (e.g., `apps/dw/contracts/*`).  
  Keep imports one-way (`apps/*` can import `core/`, not vice versa). No hardcoded settings—read from DB always.
