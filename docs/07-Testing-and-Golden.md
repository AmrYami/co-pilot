# Testing & Golden Scenarios

Golden fixtures highlight regressions for the boolean-group builder and the
`/dw/rate` SQL generator.

## Boolean group SQL

The `tests/golden/golden_boolean_groups.yaml` suite now expects the following:

* OR lists for the same field collapse into a single `IN` predicate per column.
* Alias expansions appear explicitly in the SQL (`DEPARTMENT_1 …
  OWNER_DEPARTMENT`, `CONTRACT_STAKEHOLDER_1 … _8`).
* Debug metadata includes `where_text` and `binds_text`, making it easier to
  compare the inferred structure with the generated SQL.

## Rate SQL grammar

The rate golden coverage exercises the new alias-aware equality builder.
Check `docs/RATE-GRAMMAR.md` for a quick cheatsheet of supported comment knobs
and how they map to SQL.
