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
