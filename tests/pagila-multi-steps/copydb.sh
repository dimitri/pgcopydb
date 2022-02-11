#! /bin/bash

set -x

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

coproc ( psql -At -d ${PGCOPYDB_SOURCE_PGURI} 2>&1 )

echo 'begin;' >&"${COPROC[1]}"
read x <&"${COPROC[0]}"

echo 'set transaction isolation level serializable, read only, deferrable;' >&"${COPROC[1]}"
read x <&"${COPROC[0]}"

echo 'select pg_export_snapshot();' >&"${COPROC[1]}"
read sn <&"${COPROC[0]}"

export PGCOPYDB_SNAPSHOT="${sn}"

# with a PGCOPYDB_SNAPSHOT in the environment, no need for --resume etc.
echo snapshot ${PGCOPYDB_SNAPSHOT}

pgcopydb dump schema --snapshot "${sn}"
pgcopydb restore pre-data --resume

pgcopydb copy table-data --resume
pgcopydb copy sequences --resume
pgcopydb copy indexes --resume
pgcopydb copy constraints --resume

pgcopydb restore post-data --resume

echo 'commit;' >&"${COPROC[1]}"
echo '\q' >&"${COPROC[1]}"

wait ${COPROC_PID}
