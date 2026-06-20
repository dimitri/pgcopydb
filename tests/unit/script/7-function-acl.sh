#! /bin/bash

set -x
set -e

# This script expects the following environment variables to be set:
#
#  - PGCOPYDB_TARGET_PGURI
#
# Regression test for https://github.com/dimitri/pgcopydb/issues/888
#
# pgcopydb failed when a function has an ACL entry in the pg_restore TOC,
# because these entries use the schema name (e.g. "public") as the namespace
# field instead of the dash used by SCHEMA/EXTENSION entries.  The parser
# returned false for these entries, leaving restoreListName NULL and causing
# the ACL list entry to be written with an empty name.  pg_restore then
# silently skipped the malformed entry, leaving the function ACL unrestored.
#
# The setup (18-function-acl.sql) creates test_function_acl on the source
# and REVOKEs the default PUBLIC EXECUTE.  This produces a non-default proacl
# that pg_dump includes as an explicit ACL entry in the archive.
#
# The main pgcopydb fork (copydb.sh) already cloned source to target; here
# we verify that the explicit ACL was preserved:
#   - proacl IS NOT NULL means pgcopydb applied the REVOKE from pg_restore
#   - proacl IS NULL     means the ACL entry was silently lost (regression)

psql -At -d "${PGCOPYDB_TARGET_PGURI}" <<'EOF'
SELECT CASE
    WHEN proacl IS NOT NULL
    THEN 'function ACL preserved'
    ELSE 'function ACL missing'
END
FROM pg_proc
WHERE proname = 'test_function_acl';
EOF
