#! /bin/bash

set -x
set -e

# This script expects the following environment variables to be set:
#
#  - PGCOPYDB_SOURCE_PGURI
#  - PGCOPYDB_TARGET_PGURI
#
# Column ACL and COMMENT entries have no objectOid in the pg_restore TOC, so
# filtering matches them by restore list name only. Their tag is
# "COLUMN table.column", which used to never match the owning table's
# restore list name: when the table was filtered out, its column ACL and
# COMMENT entries were still restored, and pg_restore failed with:
#
#   ERROR:  relation "public.column_acl_test" does not exist
#
# The setup (22-column-acl.sql) creates three tables with column ACL and
# COMMENT entries. Here we clone only column_acl_kept, and verify that the
# other tables' column entries are filtered out with them, including the
# fmtId() quoted table name.

psql -d "${PGCOPYDB_TARGET_PGURI}" -c "CREATE DATABASE column_acl_filtering" >&2
PGCOPYDB_TARGET_FILTERING="${PGCOPYDB_TARGET_PGURI%/*}/column_acl_filtering"

# The source fixture 19-pg-stat-statements-acl.sql grants/revokes on
# pg_stat_statements, so the dump contains an ACL for that function.
psql -d "${PGCOPYDB_TARGET_FILTERING}" -c "CREATE EXTENSION pg_stat_statements" >&2

cat > /tmp/column_acl_filtering.ini <<'EOF'
[include-only-table]
public.column_acl_kept
EOF

pgcopydb clone \
    --source "${PGCOPYDB_SOURCE_PGURI}" \
    --target "${PGCOPYDB_TARGET_FILTERING}" \
    --filters /tmp/column_acl_filtering.ini \
    --skip-collations \
    --skip-extensions \
    --skip-large-objects \
    --skip-db-properties \
    --table-jobs 1 \
    --index-jobs 1 \
    --dir /tmp/pgcopydb-column-acl-filtering \
    --fail-fast \
    --debug > /tmp/pgcopydb-column-acl-filtering.log 2>&1 \
    || (cat /tmp/pgcopydb-column-acl-filtering.log >&2; exit 1)

echo "clone with column ACL filtering succeeded"

log=/tmp/pgcopydb-column-acl-filtering.log

for desc in COMMENT ACL
do
    for name in 'public column_acl_test postgres' \
                'public column.acl "quoted" test postgres'
    do
        if grep -q "Skipping filtered-out dumpId [0-9]*: ${desc} 0 0 ${name}$" ${log}
        then
            echo "${desc} skipped for: ${name}"
        else
            echo "${desc} NOT skipped for: ${name}"
        fi
    done
done

echo "Failed to parse Archive TOC comment or acl: $(grep -c 'Failed to parse Archive TOC comment or acl' ${log} || true)"

psql -At -d "${PGCOPYDB_TARGET_FILTERING}" <<'EOF'
SELECT 'filtered-out tables on target: ' || count(*)
  FROM pg_class
 WHERE relname IN ('column_acl_test', 'column.acl "quoted" test');

SELECT CASE
    WHEN attacl IS NOT NULL
    THEN 'kept column ACL preserved'
    ELSE 'kept column ACL missing'
END
FROM pg_attribute
WHERE attrelid = 'public.column_acl_kept'::regclass
  AND attname = 'id';

SELECT CASE
    WHEN col_description('public.column_acl_kept'::regclass, attnum) IS NOT NULL
    THEN 'kept column comment preserved'
    ELSE 'kept column comment missing'
END
FROM pg_attribute
WHERE attrelid = 'public.column_acl_kept'::regclass
  AND attname = 'name';
EOF
