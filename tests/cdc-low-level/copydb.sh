#! /bin/bash

set -x
set -e
set -o pipefail

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
# PGCOPYDB_OUTPUT_PLUGIN is set to test_decoding in docker-compose.yml
coproc ( pgcopydb snapshot --follow )

sleep 1

# now setup the replication origin (target) and the pgcopydb.sentinel (source)
pgcopydb stream setup

# pgcopydb copy db uses the environment variables
pgcopydb copy-db

kill -TERM ${COPROC_PID}
wait ${COPROC_PID}

# now that the copying is done, inject some SQL DML changes to the source
psql -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/dml.sql

# grab the current LSN, it's going to be our streaming end position
lsn=`psql -At -d ${PGCOPYDB_SOURCE_PGURI} -c 'select pg_current_wal_lsn()'`

# and prefetch the changes captured in our replication slot
pgcopydb stream receive --debug --resume --endpos "${lsn}"

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
pgcopydb stream receive --debug --resume --endpos "${lsn}"

# now transform the JSON file into SQL
SQLFILENAME=`basename ${WALFILE} .json`.sql

pgcopydb stream transform --debug ${SHAREDIR}/${WALFILE} /tmp/${SQLFILENAME}

# we should also get the same result as expected (discarding LSN numbers)
DIFFOPTS='-I BEGIN -I COMMIT -I KEEPALIVE -I SWITCH'

diff ${DIFFOPTS} /usr/src/pgcopydb/${SQLFILE} /tmp/${SQLFILENAME}

# now apply the SQL file to the target database
pgcopydb stream apply --trace --resume /tmp/${SQLFILE}

# now apply AGAIN the SQL file to the target database, skipping transactions
pgcopydb stream apply --debug --resume /tmp/${SQLFILE}

#
# switching to "live streaming" tests, using unix pipes
#
# first allow applying
#
pgcopydb stream sentinel set apply

# now create some changes to replicate all over again
psql -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/dml.sql

# grab the current LSN, it's going to be our streaming end position
lsn=`psql -At -d ${PGCOPYDB_SOURCE_PGURI} -c 'select pg_current_wal_lsn()'`

# and "live replay" the changes captured in our replication slot
# avoiding pidfile clashes between three concurrent processes
pgcopydb stream receive --debug --resume --endpos "${lsn}" --to-stdout \
 | pgcopydb stream transform --debug --endpos "${lsn}" - -             \
 | pgcopydb stream apply --debug --resume --endpos "${lsn}" -

#
# now the same thing, this time using the stream replay command
#
# we do the same thing twice to verify that our client-side LSN tracking is
# done properly and allows resuming operations after reaching endpos.
#
for i in `seq 2`
do
    psql -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/dml.sql

    # grab the current LSN, it's going to be our streaming end position
    lsn=`psql -At -d ${PGCOPYDB_SOURCE_PGURI} -c 'select pg_current_wal_lsn()'`

    pgcopydb stream replay --verbose --resume --endpos "${lsn}"
done

# and check that the last time there nothing more to do
pgcopydb stream replay --resume --endpos "${lsn}"

# cleanup
pgcopydb stream cleanup
