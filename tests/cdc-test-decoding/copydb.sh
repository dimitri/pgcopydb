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

psql -o /tmp/s.out -d ${PGCOPYDB_SOURCE_PGURI} -1 -f /usr/src/pagila/pagila-schema.sql
psql -o /tmp/d.out -d ${PGCOPYDB_SOURCE_PGURI} -1 -f /usr/src/pagila/pagila-data.sql

# alter the pagila schema to allow capturing DDLs without pkey
psql -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/ddl.sql

# create the replication slot that captures all the changes
# PGCOPYDB_OUTPUT_PLUGIN is set to test_decoding in compose.yaml
coproc ( pgcopydb snapshot --follow )

sleep 1

# now setup the replication origin (target) and the pgcopydb.sentinel (source)
pgcopydb stream setup

# pgcopydb clone uses the environment variables
pgcopydb clone

kill -TERM ${COPROC_PID}
wait ${COPROC_PID}

# now that the copying is done, inject some SQL DML changes to the source
psql -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/dml.sql

# grab the current LSN, it's going to be our streaming end position
lsn=`psql -At -d ${PGCOPYDB_SOURCE_PGURI} -c 'select pg_current_wal_flush_lsn()'`

#
# Receive CDC messages into the SQLite outputDB (output table).  In the
# 2-process model the receive step only fills the output table; the transform
# into stmt+replay happens later, inside the apply (catchup) process.
#
pgcopydb stream prefetch --resume --endpos "${lsn}" --debug

SHAREDIR=/var/lib/postgres/.local/share/pgcopydb

ls -la ${SHAREDIR}/

OUTPUTDB=$(find ${SHAREDIR} -maxdepth 1 -name '*-output.db' -type f | head -1)

#
# Validate that the SQLite output table was populated by receive.
#
sqlite3 ${OUTPUTDB} "select count(*) as output_rows from output;"

# Idempotency: prefetch again should be a no-op
pgcopydb stream prefetch --resume --endpos "${lsn}" --notice

#
# Allow apply and catch up.  The apply process performs the inline transform
# (output -> stmt+replay), creating the replayDB, then applies to the target.
#
pgcopydb stream sentinel set apply
pgcopydb stream catchup --resume --endpos "${lsn}" -vv

#
# replayDB now exists: validate the SQL templates written by the transform
# step and that the replay table was populated.
#
REPLAYDB=$(find ${SHAREDIR} -maxdepth 1 -name '*-replay.db' -type f | head -1)

sqlite3 -init /dev/null -list -noheader ${REPLAYDB} \
  "select s.sql from stmt s join replay r on r.stmt_hash = s.hash where r.action not in ('B','C','R','K','X','E') group by s.hash order by min(r.id)" \
  > /tmp/stmt-actual.sql
diff /usr/src/pgcopydb/stmt.sql /tmp/stmt-actual.sql

sqlite3 ${REPLAYDB} "select count(*) as replay_rows from replay;"

# Applying again must be idempotent
pgcopydb stream catchup --resume --endpos "${lsn}" -vv

#
# Row-count validation
#
src_count=`psql -AtqX -d ${PGCOPYDB_SOURCE_PGURI} -c "select count(*) from actor"`
tgt_count=`psql -AtqX -d ${PGCOPYDB_TARGET_PGURI} -c "select count(*) from actor"`
echo "source actor count: ${src_count}, target actor count: ${tgt_count}"
test "${src_count}" -eq "${tgt_count}"

# Verify that double precision values are replayed without loss of precision
# (issue #968): the previous %f format emitted only 6 decimal places.
psql -AtqX -d ${PGCOPYDB_SOURCE_PGURI} \
     -c "select id, val::text from float8_precision_test order by id" \
     > /tmp/src_float8.txt
psql -AtqX -d ${PGCOPYDB_TARGET_PGURI} \
     -c "select id, val::text from float8_precision_test order by id" \
     > /tmp/tgt_float8.txt
diff /tmp/src_float8.txt /tmp/tgt_float8.txt

# Verify that single-quote characters inside VARCHAR and TEXT values are
# preserved correctly after CDC replay (issue #969).
psql -AtqX -d ${PGCOPYDB_SOURCE_PGURI} \
     -c "select id, varchar_col, text_col from quote_escaping_test order by id" \
     > /tmp/src_quotes.txt
psql -AtqX -d ${PGCOPYDB_TARGET_PGURI} \
     -c "select id, varchar_col, text_col from quote_escaping_test order by id" \
     > /tmp/tgt_quotes.txt
diff /tmp/src_quotes.txt /tmp/tgt_quotes.txt

#
# Stream prune between rounds: remove already-applied CDC file pairs.
# replay_lsn is now set (round 1 committed all transactions); this should
# prune the closed output.db/replay.db files whose endpos < replay_lsn.
#
pgcopydb stream prune

#
# Round 2: --replay-no-op-updates
#
# Inject a second no-op UPDATE on noop_update_test (REPLICA IDENTITY FULL).
# Run prefetch + catchup with --replay-no-op-updates and verify that the
# UPDATE now appears in the replay, unlike in the default (skip) round above.
#
psql -d ${PGCOPYDB_SOURCE_PGURI} \
     -c "UPDATE noop_update_test SET val = 'hello' WHERE id = 1;"

lsn2=`psql -At -d ${PGCOPYDB_SOURCE_PGURI} -c 'select pg_current_wal_flush_lsn()'`

pgcopydb stream prefetch --resume --endpos "${lsn2}" --debug

pgcopydb stream catchup --resume --endpos "${lsn2}" --replay-no-op-updates -vv

noop_update_count=$(sqlite3 -init /dev/null -list -noheader ${REPLAYDB} \
    "select count(*)
     from stmt s
     join replay r on r.stmt_hash = s.hash
     where s.sql like 'UPDATE public.noop_update_test%'")

echo "noop_update_count with --replay-no-op-updates: ${noop_update_count}"
test "${noop_update_count}" -ge 1

# cleanup
pgcopydb stream cleanup
