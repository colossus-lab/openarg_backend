"""Placeholder for LangGraph checkpoint tables.

LangGraph's ``AsyncPostgresSaver.setup()`` creates its own tables
(``checkpoints``, ``checkpoint_blobs``, ``checkpoint_writes``)
automatically at startup.  This migration exists solely to keep
Alembic's revision chain complete — no DDL is executed here.

Revision ID: 0023
Revises: 0022
Create Date: 2026-03-21
"""

revision = "0023"
down_revision = "0022"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # LangGraph AsyncPostgresSaver.setup() creates its own tables.
    pass


def downgrade() -> None:
    # Nothing to undo — tables are managed by LangGraph.
    pass
