--
-- Issue #888: function ACL entries in the pg_restore TOC use the schema name
-- as the namespace field (e.g. "public test_function_acl(integer, integer) owner")
-- rather than the dash used by SCHEMA/EXTENSION entries.  pgcopydb must parse
-- these correctly and preserve the ACL on the target.
--
-- We REVOKE the default PUBLIC EXECUTE to produce an explicit, non-default
-- proacl.  pg_dump only generates an ACL entry when proacl differs from the
-- initial privileges, so a simple REVOKE (not a GRANT) triggers the entry.
--
CREATE OR REPLACE FUNCTION test_function_acl(x integer, y integer)
    RETURNS integer
    LANGUAGE sql
    IMMUTABLE STRICT
AS $$ SELECT $1 + $2 $$;

-- Revoking the default PUBLIC execute privilege creates a non-default proacl
-- that pg_dump will include as an explicit ACL entry in the archive.
REVOKE EXECUTE ON FUNCTION test_function_acl(integer, integer) FROM PUBLIC;
