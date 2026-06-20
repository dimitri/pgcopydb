--
-- Issue #888 (extended): pg_stat_statements has a function with a very long
-- signature (one input parameter plus ~40 OUT parameters) that produces a
-- correspondingly long ACL entry in the pg_restore TOC.  This exercises the
-- line-reading and ACL-parsing code more thoroughly than a short signature.
--
-- We REVOKE the default PUBLIC EXECUTE to produce an explicit, non-default
-- proacl that pg_dump will include as an ACL entry in the archive.
--
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- Revoking the default PUBLIC execute privilege creates a non-default proacl
-- that pg_dump will include as an explicit ACL entry in the archive.
REVOKE EXECUTE ON FUNCTION pg_stat_statements(boolean) FROM PUBLIC;
