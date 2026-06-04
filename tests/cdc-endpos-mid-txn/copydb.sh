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
coproc ( pgcopydb snapshot --follow --slot-name ${slot} )

sleep 1

# now setup the replication origin (target) and the pgcopydb.sentinel (source)
pgcopydb stream setup

# pgcopydb clone uses the environment variables
pgcopydb clone

# now that the copying is done, inject some SQL DML changes to the source
psql -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/dml.sql

SLOT_PEEK_FILE=/tmp/repl-peek.json

# peek into the replication messages
psql -t -d ${PGCOPYDB_SOURCE_PGURI} \
    -c "SELECT data FROM pg_logical_slot_peek_changes('${slot}', NULL, NULL, 'format-version', '2', 'pretty-print', '1', 'include-lsn', '1');" \
    -o ${SLOT_PEEK_FILE}

# grab a LSN between `begin` and `commit` of the second transaction —
# that becomes our streaming end position to test partial-transaction handling
lsn=`jq -r 'select((.columns // empty) | .[] | ((.name == "category_id") and (.value == 1008))) | .lsn' ${SLOT_PEEK_FILE}`

#
# Receive CDC messages into the SQLite replayDB and transform them.
# The endpos is deliberately mid-transaction to test that the apply
# process waits for the full transaction before advancing.
#
pgcopydb stream prefetch --resume --endpos "${lsn}" --notice

SHAREDIR=/var/lib/postgres/.local/share/pgcopydb

ls -la ${SHAREDIR}/

DBFILE=$(ls ${SHAREDIR}/*.db | head -1)

#
# Validate that the SQLite output and replay tables were populated.
#
sqlite3 ${DBFILE} <<'EOF'
.echo on
select count(*) as output_rows from output;
select count(*) as replay_rows from replay;
EOF

# Idempotency: prefetch again with the same endpos should be a no-op
pgcopydb stream prefetch --resume --endpos "${lsn}" --trace

#
# Allow apply and run catchup with a short timeout —
# catchup must complete quickly since all data is already in the replayDB.
#
pgcopydb stream sentinel set apply

timeout 5s pgcopydb stream catchup --resume --endpos "${lsn}" --trace

#
# Advance endpos to current WAL position so the follow step can consume
# the remainder of the transaction that was split at the original endpos.
#
pgcopydb stream sentinel set endpos --current

# Dump replay table to diagnose lsn values before follow
DBFILE=$(ls ${SHAREDIR}/*.db | head -1)
sqlite3 ${DBFILE} "select id, action, xid, printf('%X/%X', lsn>>32, lsn&0xFFFFFFFF) as lsn from replay order by id;"

# Follow resumes from the sentinel endpos and applies all remaining changes
pgcopydb follow --resume --trace

#
# Verify that all new rows made it across (dml.sql adds rows to category).
#
sql="select count(*) from category"
test 26 -eq `psql -AtqX -d ${PGCOPYDB_SOURCE_PGURI} -c "${sql}"`
test 26 -eq `psql -AtqX -d ${PGCOPYDB_TARGET_PGURI} -c "${sql}"`

kill -TERM ${COPROC_PID}
wait ${COPROC_PID}

# cleanup
pgcopydb stream cleanup
