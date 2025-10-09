#!/usr/bin/env markdown
# CONTRIBUTING

## Branch naming
- `feature/<short>-<date>` for new features  
- `fix/<short>-<date>` for bug fixes  
- `docs/<short>-<date>` for documentation

## Pull Requests
- Include: (1) branch name, (2) files touched, (3) minimal diffs, (4) golden updates, (5) how to run tests locally.
- Add rationale in 1–2 lines (what/why), keep code comments in **English** only.

## Architecture guardrails
- `core/` = reusable, domain-agnostic. Must not import from `apps/*`.
- `apps/*` = domain code. May import `core/`, *not* vice versa.
- Table-scoped code lives under its own module within the app (e.g., `apps/dw/contracts/*`).
- No hardcoded settings — read from DB via `/admin/settings/bulk`.

## Golden first
- Every functional change must have golden coverage (WHERE/ORDER/GROUP BY direction assertions).
- Avoid regressions (no fallback listing on targeted questions).
