#!/usr/bin/env markdown
# Project Instructions — STRUCTURE & OWNERSHIP (Paste this block)

```
STRUCTURE & OWNERSHIP
- core/: Only project-agnostic, reusable building blocks (settings infra, shared DB/session factory, logging, sql utils, validators, rate grammar parser, generic research/explain). No app/domain-specific code here.
- apps/<app_name>/: Domain/app-specific code lives here (e.g., apps/dw for DocuWare).
- Table-scoped logic must live under its own files/module inside the app.
  Example (DW):
    apps/dw/contracts/
      builder.py
      fts_rules.py
      metrics.py
      tests/

DEPENDENCY RULES
- apps/* may import core/, core/ must not import apps/*.
- Table modules should not import other tables directly; factor shared logic into apps/dw/common/ or core/.
- No hardcoded settings; always read from DB (admin/settings/bulk).

WORKING STYLE
- For code changes: (1) Branch name, (2) Files, (3) Minimal diffs, (4) Golden asserts, (5) How to run tests.
- For /dw/rate: parse (fts/eq/group_by/order_by/top|bottom) and return corrected SQL in the same response with one-line explain.
- Keep code comments in English only.
```
