#! /bin/bash

set -x
set -e

# This script expects the following environment variables to be set:
#
#  - PGCOPYDB_SOURCE_PGURI
#  - PGCOPYDB_TARGET_PGURI
#
# ACL and COMMENT entries have no objectOid in the pg_restore TOC, so
# filtering matches them by restore list name only. Their tag, such as
# "TABLE foo" or "COLUMN foo.col", used to never match the restore list name
# of the table: when the table was filtered out, its ACL and COMMENT entries
# were still restored, and pg_restore failed with:
#
#   ERROR:  relation "public.column_acl_test" does not exist
#
# The setup (22-column-acl.sql) creates tables with ACL and COMMENT entries
# on themselves, their columns, index, constraint, trigger, policy, rule and
# sequence. Here we filter them out with both [include-only-table] and
# [exclude-table], and verify that their ACL and COMMENT entries are filtered
# out with them, and that the ones of the kept tables are restored.

#
# The other source fixtures are not filtering friendly, so clone from a
# dedicated source database with only our fixture.
#
source="${PGCOPYDB_SOURCE_PGURI%/*}/acl_src"

psql -d "${PGCOPYDB_SOURCE_PGURI}" -c "CREATE DATABASE acl_src" >&2
psql -d "${source}" -v ON_ERROR_STOP=1 -f ./setup/22-column-acl.sql >&2

#
# clone_filtered name filters.ini
#
# Clone the source database into a new target database using the given
# filters, then print the filtered-out ACL and COMMENT entries.
#
clone_filtered() {
    name=$1
    filters=$2
    target="${PGCOPYDB_TARGET_PGURI%/*}/${name}"
    log=/tmp/pgcopydb-${name}.log

    psql -d "${PGCOPYDB_TARGET_PGURI}" -c "CREATE DATABASE ${name}" >&2

    pgcopydb clone \
        --source "${source}" \
        --target "${target}" \
        --filters "${filters}" \
        --skip-db-properties \
        --table-jobs 1 \
        --index-jobs 1 \
        --dir /tmp/pgcopydb-${name} \
        --fail-fast \
        --debug > ${log} 2>&1 \
        || (cat ${log} >&2; exit 1)

    echo "== ${name}: clone succeeded"

    echo "Failed to parse Archive TOC comment or acl: $(grep -c 'Failed to parse Archive TOC comment or acl' ${log} || true)"

    # the filtered-out ACL and COMMENT entries, counted per restore list
    # name; the pre-data and post-data lists both log the same dumpId
    echo "filtered-out ACL and COMMENT entries:"
    sed -n 's/^.*Skipping filtered-out dumpId \([0-9]*\): \(ACL\|COMMENT\) 0 0 \(.*\)$/\1 \2 \3/p' ${log} \
        | sort -u | cut -d' ' -f2- | sort | uniq -c | sed 's/^ */  /'
}

#
# [include-only-table]: keep only column_acl_kept
#
cat > /tmp/acl_include_only.ini <<'EOF'
[include-only-table]
public.column_acl_kept
EOF

clone_filtered acl_include_only /tmp/acl_include_only.ini

psql -At -d "${PGCOPYDB_TARGET_PGURI%/*}/acl_include_only" <<'EOF'
SELECT 'filtered-out relations on target: ' || count(*)
  FROM pg_class
 WHERE relname IN ('column_acl_test', 'column.acl "quoted" test',
                   'acl_gone', 'acl_gone_id_seq');

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

#
# [exclude-table]: the other tables are kept with their ACL and COMMENT
# entries
#
cat > /tmp/acl_exclude.ini <<'EOF'
[exclude-table]
public.column_acl_test
public.acl_gone
EOF

clone_filtered acl_exclude /tmp/acl_exclude.ini

psql -At -d "${PGCOPYDB_TARGET_PGURI%/*}/acl_exclude" <<'EOF'
SELECT 'filtered-out relations on target: ' || count(*)
  FROM pg_class
 WHERE relname IN ('column_acl_test', 'acl_gone', 'acl_gone_id_seq');

SELECT CASE
    WHEN attacl IS NOT NULL
    THEN 'kept quoted column ACL preserved'
    ELSE 'kept quoted column ACL missing'
END
FROM pg_attribute
WHERE attrelid = 'public."column.acl ""quoted"" test"'::regclass
  AND attname = 'id.col';

SELECT CASE
    WHEN col_description('public."column.acl ""quoted"" test"'::regclass, attnum) IS NOT NULL
    THEN 'kept quoted column comment preserved'
    ELSE 'kept quoted column comment missing'
END
FROM pg_attribute
WHERE attrelid = 'public."column.acl ""quoted"" test"'::regclass
  AND attname = 'id.col';
EOF
