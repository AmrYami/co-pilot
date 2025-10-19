# Changelog — 2025‑10‑19

## Added
- **Trace logging for /dw/answer** that mirrors the existing `/dw/rate` logs:
  - `answer.receive`, `answer.settings.loaded`, `planner.intent.start|planner.intent`,
  - `rules.load.start|rules.load.ok`, `answer.fts.eval`, `builder.order.guard`,
  - `sql.exec.done`, `answer.response`.
  This does **not** change handlers or rotation files.

## Tests
- No behavior change to SQL; only logs added.
