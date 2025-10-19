# Admin Cycle & Feedback — Appendix (2025‑10‑19)

## Trace events for /dw/answer (mirrors /dw/rate style)

These events are emitted at **INFO** level using the existing `dw` logger and the same rotation/format:

- `answer.receive` — minimal request facts (auth_email, question_len, full_text_search).
- `answer.settings.loaded` — `fts_engine`, count of `DW_FTS_COLUMNS["Contract"]`.
- `planner.intent.start` / `planner.intent` — parsed filters, fts groups, sort fields.
- `rules.load.start` / `rules.load.ok` — rules count loaded (exact + globals, merged).
- `answer.fts.eval` — whether FTS is enabled, error string, columns_count.
- `builder.order.guard` — the final `ORDER BY …` fragment (after normalization guard).
- `sql.exec.done` — rows + duration in ms.
- `answer.response` — emitted just before sending JSON (includes `inquiry_id` when available).

> This does **not** change handlers or rotation; it only adds `logger.info({...})` calls.

### Quick grep examples
```bash
LOG=logs/log-$(date +%F).log
grep -E "'event': '(answer|planner|rules|fts|sql)\." "$LOG" | tail -n 200
```
