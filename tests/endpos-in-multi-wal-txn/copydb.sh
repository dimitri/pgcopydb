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

rm -fr /var/lib/postgres/.local/share/pgcopydb
mkdir -p /tmp/pgcopydb
pgcopydb stream cleanup
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

# pgcopydb copy db uses the environment variables
pgcopydb clone

kill -TERM ${COPROC_PID}
wait ${COPROC_PID}

# now that the copying is done, inject some SQL DML changes to the source
psql -v a=10001001  -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/multi-wal-txn.sql
psql -v a=10001002  -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/multi-wal-txn.sql

SLOT_PEEK_FILE=/tmp/repl-peek.json

# peek into the replication messages
psql -t -d ${PGCOPYDB_SOURCE_PGURI} \
    -c "SELECT data FROM pg_logical_slot_peek_changes('${slot}', NULL, NULL, 'format-version', '2', 'pretty-print', '1', 'include-lsn', '1');" \
    -o ${SLOT_PEEK_FILE}

# grab a LSN between `begin` and `commit` from second transaction, it's going to be our streaming end position
lsn_a=`jq -r 'select((.columns // empty) | .[] | ((.name == "f1") and (.value == 10001001))) | .lsn' ${SLOT_PEEK_FILE}`

lsn_b=`jq -r 'select((.columns // empty) | .[] | ((.name == "f1") and (.value == 10001002))) | .lsn' ${SLOT_PEEK_FILE}`

# and prefetch the changes captured in our replication slot
pgcopydb stream prefetch --resume --endpos "${lsn_a}" --trace

# now prefetch the changes again, which should be a noop
pgcopydb stream prefetch --resume --endpos "${lsn_a}" --trace

# now allow for replaying/catching-up changes
pgcopydb stream sentinel set apply

# now apply the SQL file to the target database shouldn't take more than 5s
timeout 5s pgcopydb stream catchup --resume --endpos "${lsn_a}" --trace

# adjust the endpos LSN to the lsn_b in the WAL
pgcopydb stream sentinel set endpos "${lsn_b}"

# and replay the available changes, including the transaction in dml2.sql now
pgcopydb follow --resume --trace

# now check that all the new rows made it
sql="select count(*) from table_a"
test 8 -eq `psql -AtqX -d ${PGCOPYDB_TARGET_PGURI} -c "${sql}"`

# adjust the endpos LSN to the current position in the WAL
pgcopydb stream sentinel set endpos --current

# and replay the available changes, including the transaction in dml2.sql now
pgcopydb follow --resume --trace

sql="select count(*) from table_a"
test 16 -eq `psql -AtqX -d ${PGCOPYDB_TARGET_PGURI} -c "${sql}"`

# cleanup
pgcopydb stream cleanup
