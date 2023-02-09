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

psql -o /tmp/d.out -d ${PGCOPYDB_SOURCE_PGURI} -1 -f /usr/src/pagila/pagila-schema.sql
psql -o /tmp/s.out -d ${PGCOPYDB_SOURCE_PGURI} -1 -f /usr/src/pagila/pagila-data.sql

psql -d ${PGCOPYDB_TARGET_PGURI} <<EOF
alter database postgres connection limit 2;
EOF

#
# pgcopydb uses the environment variables
#
# we need to export a snapshot, and keep it while the indivual steps are
# running, one at a time

coproc ( pgcopydb snapshot --debug )

sleep 1

pgcopydb dump schema --resume --debug
pgcopydb restore pre-data --resume

pgcopydb copy table-data --resume
pgcopydb copy sequences --resume
pgcopydb copy blobs --resume
pgcopydb copy indexes --resume
pgcopydb copy constraints --resume

pgcopydb restore post-data --resume

kill -TERM ${COPROC_PID}
wait ${COPROC_PID}
