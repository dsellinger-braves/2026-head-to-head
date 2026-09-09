---
name: supabase-ops
description: >-
  Operational guide for inspecting, validating, and managing Supabase database
  tables, schemas, and migrations in the 2026 Head to Head Heftystrong project.
---

# Supabase Operations Runbook

This guide covers safely querying, validating, and modifying database tables in Supabase for the `2026-head-to-head` project.

---

## 1. Environment & Credentials

- **Target Project**: `https://wczdkcdqgtzlsbssogoz.supabase.co`
- **Environment Variables**:
  - `VITE_SUPABASE_URL` / `SUPABASE_URL`
  - `VITE_SUPABASE_ANON_KEY` (public client read/write)
  - `SUPABASE_KEY` (service/anon key for Python scripts)

Always load credentials from `.env` using `python-dotenv`:
```python
import os
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase = create_client(url, key)
```

---

## 2. Table Schemas & Key Constraints

### `player_daily_stats`
- Primary Key: `id` (UUID)
- Unique Key: `UNIQUE (league_id, scoring_period_id, player_id)`
- Use `upsert` when ingesting stats to avoid duplicate constraint violations:
  ```python
  supabase.table('player_daily_stats').upsert(records, on_conflict='league_id,scoring_period_id,player_id').execute()
  ```

### `transactions`
- Primary Key: `espn_transaction_id` (TEXT)
- Use `upsert` based on `espn_transaction_id`.

### `historical_data`
- Primary Key: `_db_id` (UUID)
- Unique Key: `UNIQUE (league_id, season_year, scoring_period_id, team_id, id)`

---

## 3. Schema Verification

Before pushing code that alters data models, verify the database schema:

```bash
# Check current table columns
python check_schema.py

# Check column data types
python check_types.py
```

---

## 4. Applying Schema Migrations

When altering tables or adding indexes:
1. Write the SQL statements to a `.sql` migration file (e.g. `schema_migration.sql`).
2. If Composio Supabase toolkit is linked, you can inspect or run read-only queries:
   ```bash
   composio execute SUPABASE_RUN_READ_ONLY_QUERY -d '{"query": "SELECT count(*) FROM player_daily_stats"}'
   ```
3. Apply DDL statements in the Supabase SQL Editor:
   `https://supabase.com/dashboard/project/wczdkcdqgtzlsbssogoz/sql`
