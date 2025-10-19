# Runbook

## Troubleshooting

### `/dw/rate` doesn't create rows in `dw_feedback`
Checklist:
1. Unique index exists?  
   `CREATE UNIQUE INDEX IF NOT EXISTS ux_dw_feedback_inquiry_id ON dw_feedback(inquiry_id);`
2. Duplicates removed?  
   DELETE FROM dw_feedback a USING dw_feedback b
   WHERE a.ctid < b.ctid AND a.inquiry_id=b.inquiry_id;
3. **Schema drift** after #385: ensure columns exist
   ```sql
   ALTER TABLE dw_feedback
     ADD COLUMN IF NOT EXISTS auth_email TEXT,
     ADD COLUMN IF NOT EXISTS intent_json JSONB,
     ADD COLUMN IF NOT EXISTS binds_json JSONB,
     ADD COLUMN IF NOT EXISTS resolved_sql TEXT,
     ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'pending',
     ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW(),
     ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW();
   ```
   Re-run the request and check `SELECT inquiry_id,rating,status FROM dw_feedback ORDER BY id DESC LIMIT 5;`.

### Daily DW log events
Structured events are written to the existing daily log files (for example `logs/log-$(date +%F).log`).
No changes are required to the rotation job; the root logger simply emits richer payloads.

- `answer.receive` → `answer.settings.loaded` → `rules.load.start|ok` → `planner.intent` → `answer.fts.eval` → `builder.order.guard?` → `sql.exec.done` → `answer.response`
- `rate.intent.parsed` → `rate.persist.attempt|ok`
- `admin.approve.attempt|update.ok|rule.payload|rule.ok`
- Failures bubble up with `*.fail` events, keeping the correlation id for the request.

Quick grep helper:

```bash
LOG=logs/log-$(date +%F).log
grep -E '"event": "(answer|rules|planner|sql|rate|admin)\.' "$LOG" || tail -n 200 "$LOG"
```

If `answer.fts.eval` logs `"enabled": false` with `"error": "no_columns"`, ensure `DW_FTS_COLUMNS` are present in the current namespace (`/admin/settings/bulk`).
