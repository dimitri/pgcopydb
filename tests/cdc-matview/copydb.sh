#! /bin/bash

#
# Regression test for issue #791: CDC apply must not fail when logical
# decoding emits DML against a materialized view (which happens when the
# source runs REFRESH MATERIALIZED VIEW CONCURRENTLY).
#
# Instead of replaying the internal INSERT/UPDATE/DELETE events against the
# target matview (which Postgres rejects with "cannot change materialized
# view"), pgcopydb detects that the target relation is a matview and executes
# REFRESH MATERIALIZED VIEW on the target instead.
#

set -x
set -e

pgcopydb ping

# Build the source schema (table + matview with unique index for CONCURRENT).
psql -a -d "${PGCOPYDB_SOURCE_PGURI}" -1 -f /usr/src/pgcopydb/source.sql

# Build the target schema: same table + matview, initially empty.
psql -a -d "${PGCOPYDB_TARGET_PGURI}" -1 -f /usr/src/pgcopydb/target.sql

# Create the replication slot before the initial copy.
coproc ( pgcopydb snapshot --follow --plugin test_decoding )

sleep 1

pgcopydb stream setup
pgcopydb copy table-data

# Refresh mv1 on target to match source after the initial data copy.
# pgcopydb copy table-data copies src rows but not materialized view data.
psql -a -d "${PGCOPYDB_TARGET_PGURI}" -c "REFRESH MATERIALIZED VIEW mv1"

kill -TERM ${COPROC_PID}
wait ${COPROC_PID}

# Confirm initial copy: source mv1 = target mv1.
src_initial=$(psql -AtqX -d "${PGCOPYDB_SOURCE_PGURI}" \
                   -c 'select count(*) from mv1')
tgt_initial=$(psql -AtqX -d "${PGCOPYDB_TARGET_PGURI}" \
                   -c 'select count(*) from mv1')

if [ "${src_initial}" != "${tgt_initial}" ]; then
    echo "FAIL: initial copy matview row counts differ " \
         "(src=${src_initial}, tgt=${tgt_initial})"
    exit 1
fi

# Phase 1: insert new rows into the source table then refresh the matview
# concurrently.  The concurrent refresh generates INSERT/UPDATE/DELETE CDC
# events against the matview relation.  Without the fix, catchup would abort
# with "ERROR: cannot change materialized view".
psql -a -d "${PGCOPYDB_SOURCE_PGURI}" <<'SQL'
begin;
insert into src values (4, 'delta'), (5, 'epsilon');
commit;

refresh materialized view concurrently mv1;
SQL

lsn=$(psql -At -d "${PGCOPYDB_SOURCE_PGURI}" \
           -c 'select pg_current_wal_flush_lsn()')

pgcopydb stream prefetch --resume --endpos "${lsn}" --notice
pgcopydb stream sentinel set apply
pgcopydb stream catchup --resume --endpos "${lsn}" --notice

# Verify matview content matches between source and target.
psql -AtqX -d "${PGCOPYDB_SOURCE_PGURI}" \
     -c 'select id, val from mv1 order by id' \
     > /tmp/src_mv1.txt

psql -AtqX -d "${PGCOPYDB_TARGET_PGURI}" \
     -c 'select id, val from mv1 order by id' \
     > /tmp/tgt_mv1.txt

diff /tmp/src_mv1.txt /tmp/tgt_mv1.txt

pgcopydb stream cleanup
