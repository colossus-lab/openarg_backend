"""Audit `DROP TABLE cache_*` via a pg_event_trigger.

Revision ID: 0036
Revises: 0035
Create Date: 2026-04-25

Investigates the 144 'Table missing: marked for re-download' cases (root
cause unknown). Logs every drop of a public.cache_* table along with
session info, so the next time the spike happens we can attribute it.
"""

from __future__ import annotations

from alembic import op

revision = "0036"
down_revision = "0035"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS cache_drop_audit (
            id BIGSERIAL PRIMARY KEY,
            object_name TEXT NOT NULL,
            session_user TEXT,
            current_user TEXT,
            client_addr TEXT,
            application_name TEXT,
            query TEXT,
            dropped_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS ix_cache_drop_audit_dropped_at
            ON cache_drop_audit(dropped_at);
        """
    )
    op.execute(
        """
        CREATE OR REPLACE FUNCTION audit_cache_drop()
        RETURNS event_trigger AS $$
        DECLARE
            r RECORD;
            session_info RECORD;
        BEGIN
            FOR r IN SELECT * FROM pg_event_trigger_dropped_objects() LOOP
                IF r.object_type = 'table'
                   AND r.schema_name = 'public'
                   AND r.object_identity LIKE 'public.cache\\_%' ESCAPE '\\' THEN
                    SELECT pg_backend_pid() AS pid,
                           current_setting('application_name', TRUE) AS app,
                           inet_client_addr() AS client_addr
                      INTO session_info;
                    INSERT INTO cache_drop_audit (
                        object_name, session_user, current_user,
                        client_addr, application_name, query
                    ) VALUES (
                        r.object_identity,
                        SESSION_USER,
                        CURRENT_USER,
                        session_info.client_addr::text,
                        session_info.app,
                        current_query()
                    );
                END IF;
            END LOOP;
        END;
        $$ LANGUAGE plpgsql;
        """
    )
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1
                FROM pg_roles
                WHERE rolname = CURRENT_USER
                  AND rolsuper
            ) THEN
                EXECUTE 'DROP EVENT TRIGGER IF EXISTS trg_audit_cache_drop';
                EXECUTE 'CREATE EVENT TRIGGER trg_audit_cache_drop
                         ON sql_drop
                         EXECUTE FUNCTION audit_cache_drop()';
            ELSE
                RAISE NOTICE 'Skipping trg_audit_cache_drop creation: CURRENT_USER lacks SUPERUSER';
            END IF;
        EXCEPTION
            WHEN insufficient_privilege THEN
                RAISE NOTICE 'Skipping trg_audit_cache_drop creation: insufficient privilege';
        END;
        $$;
        """
    )


def downgrade() -> None:
    op.execute("DROP EVENT TRIGGER IF EXISTS trg_audit_cache_drop")
    op.execute("DROP FUNCTION IF EXISTS audit_cache_drop()")
    op.execute("DROP TABLE IF EXISTS cache_drop_audit")
