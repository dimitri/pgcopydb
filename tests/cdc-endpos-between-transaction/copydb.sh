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

# grab a LSN between `begin` and `commit` from second transaction, it's going to be our streaming end position
lsn=`jq -r 'select((.columns // empty) | .[] | ((.name == "category_id") and (.value == 1008))) | .lsn' ${SLOT_PEEK_FILE}`

# and prefetch the changes captured in our replication slot
pgcopydb stream prefetch --resume --endpos "${lsn}" --notice

SHAREDIR=/var/lib/postgres/.local/share/pgcopydb
WALFILE=000000010000000000000002.json
SQLFILE=000000010000000000000002.sql

# now compare JSON output, skipping the lsn and nextlsn fields which are
# different at each run
expected=/tmp/expected.json
result=/tmp/result.json

JQSCRIPT='del(.lsn) | del(.nextlsn) | del(.timestamp) | del(.xid)'

jq "${JQSCRIPT}" /usr/src/pgcopydb/${WALFILE} > ${expected}
jq "${JQSCRIPT}" ${SHAREDIR}/${WALFILE} > ${result}

# first command to provide debug information, second to stop when returns non-zero
diff ${expected} ${result} || cat ${SHAREDIR}/${WALFILE}
diff ${expected} ${result}

# now prefetch the changes again, which should be a noop
pgcopydb stream prefetch --resume --endpos "${lsn}" --trace

# recheck output (should have been a noop)
jq "${JQSCRIPT}" /usr/src/pgcopydb/${WALFILE} > ${expected}
jq "${JQSCRIPT}" ${SHAREDIR}/${WALFILE} > ${result}

diff ${expected} ${result} || cat ${SHAREDIR}/${WALFILE}
diff ${expected} ${result}

# now transform the JSON file into SQL
SQLFILENAME=`basename ${WALFILE} .json`.sql

pgcopydb stream transform --trace ${SHAREDIR}/${WALFILE} /tmp/${SQLFILENAME}

# we should get the same result as `pgcopydb stream prefetch`
diff ${SHAREDIR}/${SQLFILE} /tmp/${SQLFILENAME}

# we should also get the same result as expected (discarding LSN numbers)
DIFFOPTS='-I BEGIN -I COMMIT -I KEEPALIVE -I SWITCH -I ENDPOS -I ROLLBACK'

diff ${DIFFOPTS} /usr/src/pgcopydb/${SQLFILE} ${SHAREDIR}/${SQLFILENAME}

# now allow for replaying/catching-up changes
pgcopydb stream sentinel set apply

# now apply the SQL file to the target database shouldn't take more than 2s
timeout 5s pgcopydb stream catchup --resume --endpos "${lsn}" --trace

# adjust the endpos LSN to the current position in the WAL
pgcopydb stream sentinel set endpos --current

# and replay the available changes, including the transaction in dml2.sql now
pgcopydb follow --resume --trace

# now check that all the new rows made it
sql="select count(*) from category"
test 26 -eq `psql -AtqX -d ${PGCOPYDB_SOURCE_PGURI} -c "${sql}"`

kill -TERM ${COPROC_PID}
wait ${COPROC_PID}

# cleanup
pgcopydb stream cleanup
