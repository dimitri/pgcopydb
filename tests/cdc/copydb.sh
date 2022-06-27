#! /bin/bash

set -x
set -e

# This script expects the following environment variables to be set:
#
#  - PGCOPYDB_SOURCE_PGURI
#  - PGCOPYDB_TARGET_PGURI
#  - PGCOPYDB_TARGET_TABLE_JOBS
#  - PGCOPYDB_TARGET_INDEX_JOBS


#
# pgcopydb list tables include a retry loop, so we use that as a proxy to
# depend on the source/target Postgres images to be ready
#
pgcopydb list tables --source ${PGCOPYDB_SOURCE_PGURI}
pgcopydb list tables --source ${PGCOPYDB_TARGET_PGURI}

psql -d ${PGCOPYDB_SOURCE_PGURI} -1 -f /usr/src/pagila/pagila-schema.sql
psql -d ${PGCOPYDB_SOURCE_PGURI} -1 -f /usr/src/pagila/pagila-data.sql

# alter the pagila schema to allow capturing DDLs without pkey
psql -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/ddl.sql

# create the replication slot that captures all the changes
psql -d ${PGCOPYDB_SOURCE_PGURI} <<EOF
begin;
SELECT 'init' FROM pg_create_logical_replication_slot('test_slot', 'wal2json');
commit;
EOF

# pgcopydb copy db uses the environment variables
pgcopydb copy-db

# now that the copying is done, inject some SQL DML changes to the source
psql -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/dml.sql

# grab the current LSN, it's going to be our streaming end position
lsn=`psql -At -d ${PGCOPYDB_SOURCE_PGURI} -c 'select pg_current_wal_lsn()'`

# and receive the changes captured in our replication slot
pgcopydb stream receive --slot-name test_slot --restart --endpos "${lsn}" -vv

SHAREDIR=/var/lib/postgres/.local/share/pgcopydb
WALFILE=000000010000000000000002.json

cat ${SHAREDIR}/${WALFILE}

# now compare JSON output, skipping the lsn and nextlsn fields which are
# different at each run
expected=/tmp/expected.json
result=/tmp/result.json

JQSCRIPT='del(.lsn) | del(.nextlsn) | del(.timestamp)'

jq "${JQSCRIPT}" /usr/src/pgcopydb/${WALFILE} > ${expected}
jq "${JQSCRIPT}" ${SHAREDIR}/${WALFILE} > ${result}

diff ${expected} ${result}

# now transform the JSON file into SQL
SQLFILE=`basename ${WALFILE} .json`.sql

pgcopydb stream transform -vvv ${SHAREDIR}/${WALFILE} ${SHAREDIR}/${SQLFILE}

DIFFOPTS='-I BEGIN -I COMMIT'

diff ${DIFFOPTS} /usr/src/pgcopydb/${SQLFILE} ${SHAREDIR}/${SQLFILE}
