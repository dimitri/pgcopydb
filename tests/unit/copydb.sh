#! /bin/bash

set -x
set -e

# This script expects the following environment variables to be set:
#
#  - PGCOPYDB_SOURCE_PGURI
#  - PGCOPYDB_TARGET_PGURI
#  - PGCOPYDB_TABLE_JOBS
#  - PGCOPYDB_INDEX_JOBS

# make sure source and target databases are ready
pgcopydb ping

sql='ALTER DATABASE postgres SET search_path TO public, """abc""";'
psql -a -d "${PGCOPYDB_SOURCE_PGURI}" -c "${sql}"
psql -a -d "${PGCOPYDB_SOURCE_PGURI}" -1 -f ./setup/setup.sql

# create the target needed collation manually for the test
psql -a -d "${PGCOPYDB_TARGET_PGURI}" -1 <<EOF
create collation if not exists mycol
 (
   locale = 'fr-FR-x-icu',
   provider = 'icu'
 );
EOF

# pgcopydb fork uses the environment variables
export PGCOPYDB_SPLIT_TABLES_LARGER_THAN="2MB"
pgcopydb fork --skip-collations --fail-fast --debug


# now compare the output of running the SQL command with what's expected
# as we're not root when running tests, can't write in /usr/src
mkdir -p /tmp/results

find .

pgopts="--single-transaction --no-psqlrc --expanded"

for f in ./sql/*.sql
do
    t=`basename $f .sql`
    r=/tmp/results/${t}.out
    e=./expected/${t}.out
    psql -d "${PGCOPYDB_TARGET_PGURI}" ${pgopts} --file ./sql/$t.sql &> $r
    test -f $e || cat $r
    diff $e $r || exit 1
done


for f in ./script/*.sh
do
    t=`basename $f .sh`
    r=/tmp/results/${t}.out
    e=./expected/${t}.out
    bash $f > $r
    test -f $e || cat $r
    # exclude logs, whitespaces and blank lines
    DIFFOPTS='-B -w -I INFO -I WARN'
    diff ${DIFFOPTS} $e $r || cat $r
    diff ${DIFFOPTS} $e $r || exit 1
done


# ============================================================
# Regression test for issue #894: binary COPY with tsvector
#
# Clone only public.tsv into a fresh database using
# --use-copy-binary.  pgcopydb must detect that tsvectorsend is
# on the blocklist, emit a per-table warning, fall back to text
# COPY, and deliver the row intact.
#
# On macOS arm64 (Apple Silicon) without the fix, this clone
# would crash the PostgreSQL backend (SIGBUS / ASAN alignment
# fault in tsvectorrecv).  On Linux arm64/amd64 CI without an
# ASAN-instrumented PostgreSQL build the crash does not manifest,
# but the test still validates the fallback behaviour: the
# blocklist warning must appear and the data must arrive.
# ============================================================

psql -a -d "${PGCOPYDB_TARGET_PGURI}" -c "CREATE DATABASE tsv_binary_test"
PGCOPYDB_TARGET_BINARY="${PGCOPYDB_TARGET_PGURI%/*}/tsv_binary_test"

# The source fixture 19-pg-stat-statements-acl.sql grants/revokes on
# pg_stat_statements, so the dump contains an ACL for that function.
# Pre-create the extension in the fresh database so pg_restore can apply it.
psql -a -d "${PGCOPYDB_TARGET_BINARY}" -c "CREATE EXTENSION pg_stat_statements"

cat > /tmp/tsv_only.ini <<'FILTEREOF'
[include-only-table]
public.tsv
FILTEREOF

pgcopydb clone \
    --source "${PGCOPYDB_SOURCE_PGURI}" \
    --target "${PGCOPYDB_TARGET_BINARY}" \
    --use-copy-binary \
    --filters /tmp/tsv_only.ini \
    --skip-collations \
    --skip-extensions \
    --skip-large-objects \
    --skip-db-properties \
    --table-jobs 1 \
    --index-jobs 1 \
    --dir /tmp/pgcopydb-binary-test \
    --fail-fast \
    --debug 2>&1 | tee /tmp/pgcopydb-binary-test.log

count=$(psql -t -A -d "${PGCOPYDB_TARGET_BINARY}" \
    -c "SELECT count(*) FROM public.tsv WHERE id = 1;")

if [ "${count}" != "1" ]; then
    echo "ERROR: issue-894 binary tsvector test: expected 1 row, got ${count}"
    exit 1
fi

if ! grep -q "not safe for COPY BINARY" /tmp/pgcopydb-binary-test.log; then
    echo "ERROR: issue-894 binary tsvector test: blocklist warning not found in output"
    cat /tmp/pgcopydb-binary-test.log
    exit 1
fi

echo "issue-894 binary tsvector regression test: PASSED"
