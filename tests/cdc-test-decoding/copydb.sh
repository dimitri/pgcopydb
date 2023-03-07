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
# PGCOPYDB_OUTPUT_PLUGIN is set to test_decoding in docker-compose.yml
pgcopydb stream setup

# pgcopydb copy db uses the environment variables
pgcopydb copy-db

# now that the copying is done, inject some SQL DML changes to the source
psql -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/dml.sql

# grab the current LSN, it's going to be our streaming end position
lsn=`psql -At -d ${PGCOPYDB_SOURCE_PGURI} -c 'select pg_current_wal_lsn()'`

# and prefetch the changes captured in our replication slot
pgcopydb stream prefetch --resume --endpos "${lsn}" -vv

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
pgcopydb stream prefetch --resume --endpos "${lsn}" -vv

# now transform the JSON file into SQL
SQLFILENAME=`basename ${WALFILE} .json`.sql

pgcopydb stream transform --debug ${SHAREDIR}/${WALFILE} /tmp/${SQLFILENAME}

# we should get the same result as `pgcopydb stream prefetch`
diff ${SHAREDIR}/${SQLFILE} /tmp/${SQLFILENAME}

# we should also get the same result as expected (discarding LSN numbers)
DIFFOPTS='-I BEGIN -I COMMIT -I KEEPALIVE -I SWITCH'

diff ${DIFFOPTS} /usr/src/pgcopydb/${SQLFILE} ${SHAREDIR}/${SQLFILENAME}

# now allow for replaying/catching-up changes
pgcopydb stream sentinel set apply

# now apply the SQL file to the target database
pgcopydb stream catchup --resume --endpos "${lsn}" -vv

# now apply AGAIN the SQL file to the target database, skipping transactions
pgcopydb stream catchup --resume --endpos "${lsn}" -vv

# cleanup
pgcopydb stream cleanup
