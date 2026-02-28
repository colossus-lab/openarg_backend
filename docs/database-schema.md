# Database Schema

PostgreSQL 16 with pgvector extension. All primary keys are UUID v4 with `gen_random_uuid()` server default.

## Entity-Relationship Diagram

```
┌──────────┐       ┌────────────────┐       ┌──────────┐
│  users   │──1:N──│ conversations  │──1:N──│ messages  │
└──────────┘       └────────────────┘       └──────────┘

┌──────────┐       ┌────────────────┐       ┌──────────────────┐
│ datasets │──1:N──│ dataset_chunks │       │ cached_datasets   │
│          │──1:N──│                │       │  (download cache) │
└──────────┘       └────────────────┘       └──────────────────┘
                          ↑ pgvector(1536)

┌──────────────┐       ┌──────────────────────┐       ┌──────────────┐
│ user_queries │──1:N──│ query_dataset_links   │       │ agent_tasks  │
│  (analytics) │──1:N──│                       │       │              │
└──────────────┘       └──────────────────────┘       └──────────────┘
```

## Tables

### `users`

Synced from Google OAuth (NextAuth frontend).

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `id` | UUID | PK, default `gen_random_uuid()` | |
| `email` | VARCHAR(320) | UNIQUE, NOT NULL | Google email |
| `name` | VARCHAR(500) | NOT NULL, default `''` | Display name |
| `image_url` | VARCHAR(2000) | nullable | Google profile image URL |
| `created_at` | TIMESTAMPTZ | default `now()` | |
| `updated_at` | TIMESTAMPTZ | default `now()` | |

### `conversations`

Chat conversations linked to a user.

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `id` | UUID | PK, default `gen_random_uuid()` | |
| `user_id` | UUID | FK → `users.id` ON DELETE CASCADE, INDEX | Owner |
| `title` | VARCHAR(1000) | NOT NULL, default `''` | Conversation title |
| `created_at` | TIMESTAMPTZ | default `now()` | |
| `updated_at` | TIMESTAMPTZ | default `now()` | |

**Indexes:** `ix_conversations_user_id` on `user_id`

### `messages`

Individual messages within a conversation.

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `id` | UUID | PK, default `gen_random_uuid()` | |
| `conversation_id` | UUID | FK → `conversations.id` ON DELETE CASCADE, INDEX | |
| `role` | VARCHAR(20) | NOT NULL | `"user"` or `"assistant"` |
| `content` | TEXT | NOT NULL | Message text |
| `sources` | JSONB | nullable, default `'[]'::jsonb` | Source attributions |
| `created_at` | TIMESTAMPTZ | default `now()` | |
| `updated_at` | TIMESTAMPTZ | default `now()` | |

**Indexes:** `ix_messages_conversation_id` on `conversation_id`

### `datasets`

Metadata for indexed public datasets.

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `id` | UUID | PK, default `gen_random_uuid()` | |
| `source_id` | VARCHAR(500) | NOT NULL | Portal-specific dataset ID |
| `title` | VARCHAR(1000) | NOT NULL | |
| `description` | TEXT | nullable | |
| `organization` | VARCHAR(500) | nullable | Publishing organization |
| `portal` | VARCHAR(100) | NOT NULL, INDEX | Source portal identifier |
| `url` | TEXT | nullable | Dataset page URL |
| `download_url` | TEXT | nullable | Direct download URL |
| `format` | VARCHAR(50) | nullable | File format (csv, json, xlsx) |
| `columns` | TEXT | nullable | JSON string with column names |
| `sample_rows` | TEXT | nullable | JSON string with sample data |
| `tags` | TEXT | nullable | Comma-separated tags |
| `last_updated_at` | TIMESTAMPTZ | nullable | Last update on portal |
| `is_cached` | BOOLEAN | default `false` | Whether data is cached locally |
| `row_count` | INTEGER | nullable | Number of rows |
| `created_at` | TIMESTAMPTZ | default `now()` | |
| `updated_at` | TIMESTAMPTZ | default `now()`, onupdate `now()` | |

**Indexes:** `ix_datasets_source_portal` UNIQUE on `(source_id, portal)`

### `dataset_chunks`

Vector-embedded chunks for semantic search.

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `id` | UUID | PK, default `gen_random_uuid()` | |
| `dataset_id` | UUID | NOT NULL, INDEX | Reference to parent dataset |
| `content` | TEXT | NOT NULL | Chunk text (metadata combination) |
| `embedding` | vector(1536) | pgvector | OpenAI text-embedding-3-small |
| `created_at` | TIMESTAMPTZ | default `now()` | |
| `updated_at` | TIMESTAMPTZ | default `now()` | |

**Indexes:** `ix_dataset_chunks_embedding` HNSW using `vector_cosine_ops` (m=16, ef_construction=64)

Each dataset generates 3 chunks:
1. **Main metadata** — title + description + organization + tags + format + row_count
2. **Column-focused** — column names + contextual info
3. **Use-case** — how to query the data, availability context

### `cached_datasets`

Tracks downloaded and cached dataset files.

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `id` | UUID | PK, default `gen_random_uuid()` | |
| `dataset_id` | UUID | NOT NULL, INDEX | Reference to parent dataset |
| `table_name` | VARCHAR(255) | NOT NULL, UNIQUE | PostgreSQL table name (`cache_*`) |
| `row_count` | INTEGER | default `0` | |
| `columns_json` | TEXT | nullable | JSON with column info |
| `size_bytes` | INTEGER | default `0` | |
| `status` | VARCHAR(50) | default `'pending'` | pending/downloading/ready/error |
| `error_message` | TEXT | nullable | Error details if status=error |
| `created_at` | TIMESTAMPTZ | default `now()` | |
| `updated_at` | TIMESTAMPTZ | default `now()`, onupdate `now()` | |

### `user_queries`

Analytics log for all queries (kept separate from conversations).

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `id` | UUID | PK, default `gen_random_uuid()` | |
| `user_id` | TEXT | nullable, INDEX | Session ID or email |
| `question` | TEXT | NOT NULL | Original question |
| `status` | VARCHAR(50) | INDEX, default `'pending'` | Query lifecycle status |
| `plan_json` | TEXT | nullable | LLM-generated execution plan |
| `datasets_used` | TEXT | nullable | JSON array of dataset IDs |
| `analysis_result` | TEXT | nullable | Final LLM analysis |
| `sources_json` | TEXT | nullable | JSON array of source attributions |
| `error_message` | TEXT | nullable | |
| `tokens_used` | INTEGER | default `0` | Total LLM tokens consumed |
| `duration_ms` | INTEGER | default `0` | Total processing time |
| `created_at` | TIMESTAMPTZ | default `now()` | |
| `updated_at` | TIMESTAMPTZ | default `now()` | |

### `query_dataset_links`

Many-to-many between queries and datasets with relevance score.

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `id` | UUID | PK, default `gen_random_uuid()` | |
| `query_id` | UUID | NOT NULL, INDEX | |
| `dataset_id` | UUID | NOT NULL, INDEX | |
| `relevance_score` | FLOAT | default `0.0` | Vector search similarity score |
| `created_at` | TIMESTAMPTZ | default `now()` | |
| `updated_at` | TIMESTAMPTZ | default `now()` | |

### `agent_tasks`

Execution log for individual agent steps within a query.

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `id` | UUID | PK, default `gen_random_uuid()` | |
| `query_id` | UUID | NOT NULL, INDEX | Parent query |
| `agent_type` | VARCHAR(50) | NOT NULL, INDEX | planner/collector/analyst |
| `status` | VARCHAR(50) | default `'pending'` | pending/running/completed/error |
| `input_json` | TEXT | nullable | JSON input to the agent |
| `output_json` | TEXT | nullable | JSON output from the agent |
| `error_message` | TEXT | nullable | |
| `tokens_used` | INTEGER | default `0` | |
| `duration_ms` | INTEGER | default `0` | |
| `created_at` | TIMESTAMPTZ | default `now()` | |
| `updated_at` | TIMESTAMPTZ | default `now()` | |

### Dynamic `cache_*` tables

Created on-the-fly by the collector worker when downloading datasets. Table names follow the pattern `cache_{sanitized_dataset_title}`. Schema mirrors the original dataset columns. Limited to 500k rows.

## Migrations

Managed via Alembic. Migration files are in `src/app/infrastructure/persistence_sqla/alembic/versions/`.

| Revision | Description |
|----------|-------------|
| `0001` | Initial schema: datasets, dataset_chunks (pgvector), cached_datasets, user_queries, query_dataset_links, agent_tasks |
| `0002` | Change user_queries.user_id from UUID to TEXT |
| `0003` | Add users, conversations, messages tables with FK constraints |

### Running Migrations

```bash
# Apply all pending migrations
make db.migrate

# Create a new migration
make db.revision msg="description of changes"
```

The Alembic config is in `alembic.ini`. The env.py loads settings and maps tables before running migrations.
