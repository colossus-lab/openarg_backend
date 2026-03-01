-- Create a read-only PostgreSQL role for the SQL sandbox.
-- Run this in production to limit what the sandbox can do.
--
-- Usage:
--   psql -U postgres -d openarg_db -f scripts/create_sandbox_role.sql
--
-- Then set SANDBOX_DATABASE_URL to:
--   postgresql+psycopg://openarg_sandbox_ro:<password>@<host>:5432/openarg_db

DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'openarg_sandbox_ro') THEN
        CREATE ROLE openarg_sandbox_ro WITH LOGIN PASSWORD 'CHANGE_ME';
    END IF;
END
$$;

-- Read-only access to the database
GRANT CONNECT ON DATABASE openarg_db TO openarg_sandbox_ro;
GRANT USAGE ON SCHEMA public TO openarg_sandbox_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO openarg_sandbox_ro;

-- Auto-grant SELECT on future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO openarg_sandbox_ro;
