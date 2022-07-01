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

#
# pgcopydb uses the environment variables
#
# we need to export a snapshot, and keep it while the indivual steps are
# running, one at a time

coproc ( pgcopydb create snapshot -vv )

sleep 1

pgcopydb dump schema --resume -vv
pgcopydb restore pre-data --resume

pgcopydb copy table-data --resume
pgcopydb copy sequences --resume
pgcopydb copy blobs --resume
pgcopydb copy indexes --resume
pgcopydb copy constraints --resume

pgcopydb restore post-data --resume

kill -TERM ${COPROC_PID}
wait ${COPROC_PID}
