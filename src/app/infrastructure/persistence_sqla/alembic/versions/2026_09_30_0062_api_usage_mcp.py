"""api_usage: modo, herramienta, vía y cliente de cada pedido

Until now `api_usage` only saw `/ask`, and only when the pipeline ran: the data
mode (`/catalogo/*`, `/fuentes`) left nothing but Redis counters that expire in
48 h, rate-limit rejections were raised before anything was written, and there
was no way to tell a call from the public MCP (mcp.openarg.org) from a direct
API call, or Claude Code from Cursor from someone's own agent. The admin usage
dashboard (`/api/v1/admin/analytics/mcp/*`) needs all of that.

- `mode`: `respuestas` (the LLM pipeline) or `datos` (catalog and rows).
- `tool`: the MCP tool the endpoint backs, e.g. `buscar_datasets`.
- `via`: `mcp` when the public MCP server forwarded the call, else `api`.
- `client`: a normalised client family (`claude-code`, `cursor`, …).
- `user_agent`: truncated, kept to calibrate that normalisation.

All nullable: rows written before this revision have none of it, and the
dashboard infers `mode` for them from `endpoint`. Data-mode rows never carry
`question` — what someone searched for or which table they read is not stored.

The composite index serves the per-key and active-key queries of the dashboard.

Revision ID: 0062
Revises: 0061
"""

from alembic import op

revision = "0062"
down_revision = "0061"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE api_usage ADD COLUMN IF NOT EXISTS mode VARCHAR(12)")
    op.execute("ALTER TABLE api_usage ADD COLUMN IF NOT EXISTS tool VARCHAR(40)")
    op.execute("ALTER TABLE api_usage ADD COLUMN IF NOT EXISTS via VARCHAR(8)")
    op.execute("ALTER TABLE api_usage ADD COLUMN IF NOT EXISTS client VARCHAR(32)")
    op.execute("ALTER TABLE api_usage ADD COLUMN IF NOT EXISTS user_agent VARCHAR(160)")
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_api_usage_key_created ON api_usage (api_key_id, created_at)"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_api_usage_key_created")
    for column in ("user_agent", "client", "via", "tool", "mode"):
        op.execute(f"ALTER TABLE api_usage DROP COLUMN IF EXISTS {column}")
