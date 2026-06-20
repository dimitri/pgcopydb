#! /bin/bash

set -x
set -e

# This script expects the following environment variables to be set:
#
#  - PGCOPYDB_TARGET_PGURI
#
# Regression test for https://github.com/dimitri/pgcopydb/issues/888
#
# pg_stat_statements exposes a function whose TOC ACL entry has a very long
# line (one input parameter plus ~40 OUT parameters in the signature).  This
# exercises the PQExpBuffer-based line reader and the ACL parser more
# thoroughly than the short test_function_acl(integer, integer) in test 7.
#
# The setup (19-pg-stat-statements-acl.sql) creates the extension and REVOKEs
# the default PUBLIC EXECUTE.  This produces a non-default proacl that pg_dump
# includes as an explicit ACL entry in the archive.
#
# The main pgcopydb fork (copydb.sh) already cloned source to target; here
# we verify that the explicit ACL was preserved:
#   - proacl IS NOT NULL means pgcopydb applied the REVOKE from pg_restore
#   - proacl IS NULL     means the ACL entry was silently lost (regression)

psql -At -d "${PGCOPYDB_TARGET_PGURI}" <<'EOF'
SELECT CASE
    WHEN proacl IS NOT NULL
    THEN 'pg_stat_statements ACL preserved'
    ELSE 'pg_stat_statements ACL missing'
END
FROM pg_proc
WHERE proname = 'pg_stat_statements'
LIMIT 1;
EOF
