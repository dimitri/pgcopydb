#! /bin/bash

set -x
set -e

# Regression test for two related fixes against partitioned target tables:
#
#   1. pgcopydb copy table-data must use TRUNCATE (not TRUNCATE ONLY) and
#      skip COPY FREEZE when the target is partitioned. Otherwise the
#      initial copy errors with:
#        ERROR: cannot truncate only a partitioned table
#        ERROR: cannot perform COPY FREEZE on a partitioned table
#
#   2. CDC apply must rewrite "TRUNCATE ONLY <qname>" emitted by
#      ld_transform to plain "TRUNCATE <qname>" when the target relation
#      is partitioned. Otherwise stream catchup errors with the same
#      "cannot truncate only" message and apply stops.
#
# Source has a flat table (relkind='r'); target has a partitioned table
# (relkind='p') with the same qualified name.

pgcopydb ping

# Build the asymmetric fixture: flat on source, partitioned on target.
psql -a -d "${PGCOPYDB_SOURCE_PGURI}" -1 -f /usr/src/pgcopydb/source.sql
psql -a -d "${PGCOPYDB_TARGET_PGURI}" -1 -f /usr/src/pgcopydb/target.sql

# Snapshot + slot for follow mode, then copy the seed rows. This step
# exercises fix #1: copy table-data must succeed against the partitioned
# target.
coproc ( pgcopydb snapshot --follow --plugin test_decoding )

sleep 1

pgcopydb stream setup
pgcopydb copy table-data

kill -TERM ${COPROC_PID}
wait ${COPROC_PID}

# Confirm initial copy distributed the seed rows correctly into the
# partitions. If pg_copy had silently failed, this assertion catches it.
initial_a=$(psql -At -d "${PGCOPYDB_TARGET_PGURI}" \
                 -c 'select count(*) from partitioned_target.events_a')
initial_b=$(psql -At -d "${PGCOPYDB_TARGET_PGURI}" \
                 -c 'select count(*) from partitioned_target.events_b')

if [ "${initial_a}" != "2" ] || [ "${initial_b}" != "1" ]; then
    echo "FAIL: initial copy partition counts (events_a=${initial_a}, " \
         "events_b=${initial_b}); expected 2 and 1"
    exit 1
fi

# Phase 1: drive INSERTs through CDC and assert they reach the
# partitioned target with the correct routing.
psql -a -d "${PGCOPYDB_SOURCE_PGURI}" -f /usr/src/pgcopydb/dml.sql

lsn1=$(psql -At -d "${PGCOPYDB_SOURCE_PGURI}" \
            -c 'select pg_current_wal_flush_lsn()')

pgcopydb stream prefetch --resume --endpos "${lsn1}" --notice
pgcopydb stream sentinel set apply
pgcopydb stream catchup --resume --endpos "${lsn1}" --notice

after_inserts_a=$(psql -At -d "${PGCOPYDB_TARGET_PGURI}" \
                       -c 'select count(*) from partitioned_target.events_a')
after_inserts_b=$(psql -At -d "${PGCOPYDB_TARGET_PGURI}" \
                       -c 'select count(*) from partitioned_target.events_b')

# Source has 5 rows: 3 seed (id 1..3) + 2 new (id 4..5).
# bucket=0 -> events_a: ids 1,3,5 = 3 rows
# bucket=1 -> events_b: ids 2,4   = 2 rows
if [ "${after_inserts_a}" != "3" ] || [ "${after_inserts_b}" != "2" ]; then
    echo "FAIL: after CDC inserts (events_a=${after_inserts_a}, " \
         "events_b=${after_inserts_b}); expected 3 and 2"
    exit 1
fi

# Phase 2: drive a TRUNCATE through CDC. Without fix #2 catchup errors
# here; with the fix the target ends up empty.
psql -a -d "${PGCOPYDB_SOURCE_PGURI}" -f /usr/src/pgcopydb/dml-truncate.sql

lsn2=$(psql -At -d "${PGCOPYDB_SOURCE_PGURI}" \
            -c 'select pg_current_wal_flush_lsn()')

pgcopydb stream prefetch --resume --endpos "${lsn2}" --notice
pgcopydb stream catchup --resume --endpos "${lsn2}" --notice

pgcopydb stream cleanup

final_count=$(psql -At -d "${PGCOPYDB_TARGET_PGURI}" \
                   -c 'select count(*) from partitioned_target.events')

if [ "${final_count}" != "0" ]; then
    echo "FAIL: expected target empty after TRUNCATE replay, got ${final_count}"
    exit 1
fi
