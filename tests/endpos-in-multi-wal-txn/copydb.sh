#! /bin/bash

set -x
set -e

# This script expects the following environment variables to be set:
#
#  - PGCOPYDB_SOURCE_PGURI
#  - PGCOPYDB_TARGET_PGURI
#  - PGCOPYDB_TABLE_JOBS
#  - PGCOPYDB_INDEX_JOBS
#  - PGCOPYDB_OUTPUT_PLUGIN

env | grep ^PGCOPYDB

# make sure source and target databases are ready
pgcopydb ping

psql -o /tmp/s.out -d ${PGCOPYDB_SOURCE_PGURI} -1 -f /usr/src/pagila/pagila-schema.sql
psql -o /tmp/d.out -d ${PGCOPYDB_SOURCE_PGURI} -1 -f /usr/src/pagila/pagila-data.sql

# alter the pagila schema to allow capturing DDLs without pkey
psql -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/ddl.sql

slot=pgcopydb

# create the replication slot that captures all the changes
# PGCOPYDB_OUTPUT_PLUGIN is set to wal2json in docker-compose.yml
coproc ( pgcopydb snapshot --follow --slot-name ${slot})

sleep 1

# now setup the replication origin (target) and the pgcopydb.sentinel (source)
pgcopydb stream setup

# pgcopydb clone uses the environment variables
pgcopydb clone --use-binary-copy

# now that the copying is done, inject some SQL DML changes to the source
psql -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/multi-wal-txn.sql

SLOT_PEEK_FILE=/tmp/repl-peek.json

# peek into the replication messages
psql -t -d ${PGCOPYDB_SOURCE_PGURI} \
    -c "SELECT data FROM pg_logical_slot_peek_changes('${slot}', NULL, NULL, 'format-version', '2', 'pretty-print', '1', 'include-lsn', '1');" \
    -o ${SLOT_PEEK_FILE}

# LSN of first insert in a new WAL segement.
lsn_a=`jq -r 'select((.columns // empty) | .[] | ((.name == "f1") and (.value == 10001001))) | .lsn' ${SLOT_PEEK_FILE} | tail -1`

# and prefetch the changes captured in our replication slot
pgcopydb stream prefetch --resume --endpos "${lsn_a}" --trace

# now prefetch the changes again, which should be a noop
pgcopydb stream prefetch --resume --endpos "${lsn_a}" --trace

# now allow for replaying/catching-up changes
pgcopydb stream sentinel set apply

# now apply the SQL file to the target database shouldn't take more than 5s
timeout 5s pgcopydb stream catchup --resume --endpos "${lsn_a}" --trace

psql -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/multi-wal-txn.sql
psql -t -d ${PGCOPYDB_SOURCE_PGURI} \
    -c "SELECT data FROM pg_logical_slot_peek_changes('${slot}', NULL, NULL, 'format-version', '2', 'pretty-print', '1', 'include-lsn', '1');" \
    -o ${SLOT_PEEK_FILE}
# LSN of middle insert in a WAL segement.
lsn_b=`jq -r 'select((.columns // empty) | .[] | ((.name == "f1") and (.value == 10001002))) | .lsn' ${SLOT_PEEK_FILE} | tail -1`

# adjust the endpos LSN to the lsn_b in the WAL
pgcopydb stream sentinel set endpos "${lsn_b}"

# and replay the available changes
pgcopydb follow --resume --trace

# now check that all the new rows made it
sql="select count(*) from table_a"
test 8 -eq `psql -AtqX -d ${PGCOPYDB_TARGET_PGURI} -c "${sql}"`

# adjust the endpos LSN to the current position in the WAL
pgcopydb stream sentinel set endpos --current

# and replay the available changes, including the second txn.
pgcopydb follow --resume --trace

# now check that all the new rows made it
sql="select count(*) from table_a"
test 16 -eq `psql -AtqX -d ${PGCOPYDB_TARGET_PGURI} -c "${sql}"`

psql -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/multi-wal-txn.sql
psql -t -d ${PGCOPYDB_SOURCE_PGURI} \
    -c "SELECT data FROM pg_logical_slot_peek_changes('${slot}', NULL, NULL, 'format-version', '2', 'pretty-print', '1', 'include-lsn', '1');" \
    -o ${SLOT_PEEK_FILE}

# LSN of the last insert in a WAL segement.
lsn_c=`jq -r 'select((.columns // empty) | .[] | ((.name == "f1") and (.value == 10001003))) | .lsn' ${SLOT_PEEK_FILE} | tail -1`

# adjust the endpos LSN to the current position in the WAL
pgcopydb stream sentinel set endpos "${lsn_c}"

# and replay the available changes, including the 3rd txn.
pgcopydb follow --resume --trace

# new txn should not have made it yet
sql="select count(*) from table_a"
test 16 -eq `psql -AtqX -d ${PGCOPYDB_TARGET_PGURI} -c "${sql}"`

pgcopydb stream sentinel set endpos --current
# and replay the available changes, including the 3rd txn.
pgcopydb follow --resume --trace

# This operation is expected to be a no-op. It tests the scenario where,
# upon resuming, we skip a transaction that lacks a commitLSN in its
# BEGIN message but has already been applied.
pgcopydb stream apply --trace --resume /var/lib/postgres/.local/share/pgcopydb/000000010000000000000004.sql

# now check that all the new rows made it
sql="select count(*) from table_a"
test 24 -eq `psql -AtqX -d ${PGCOPYDB_TARGET_PGURI} -c "${sql}"`

kill -TERM ${COPROC_PID}
wait ${COPROC_PID}

# cleanup
pgcopydb stream cleanup
