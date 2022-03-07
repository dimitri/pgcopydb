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

psql -d ${PGCOPYDB_SOURCE_PGURI} -1 -f /usr/src/pgcopydb/import.sql

psql -d ${PGCOPYDB_SOURCE_PGURI} -1 -c 'table pg_largeobject_metadata'
psql -d ${PGCOPYDB_TARGET_PGURI} -1 -c 'table pg_largeobject_metadata'

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

# pgcopydb restore pre-data have created the large objects already
psql -d ${PGCOPYDB_TARGET_PGURI} -1 -c 'table pg_largeobject_metadata'

pgcopydb copy blobs --resume

pgcopydb restore post-data --resume

echo 'commit;' >&"${COPROC[1]}"
echo '\q' >&"${COPROC[1]}"

wait ${COPROC_PID}

SQL="select loid, count(data) as parts, sum(length(data)) as size from pg_largeobject group by loid order by loid;"

psql -d ${PGCOPYDB_SOURCE_PGURI} -1 -c "${SQL}" > /tmp/source.lo
psql -d ${PGCOPYDB_TARGET_PGURI} -1 -c "${SQL}" > /tmp/target.lo

diff /tmp/source.lo /tmp/target.lo
