#! /bin/bash

set -x
set -e

# This script expects the following environment variables to be set:
#
#  - PGCOPYDB_SOURCE_PGURI
#  - PGCOPYDB_TARGET_PGURI
#  - PGCOPYDB_TABLE_JOBS
#  - PGCOPYDB_INDEX_JOBS


#
# pgcopydb list tables include a retry loop, so we use that as a proxy to
# depend on the source/target Postgres images to be ready
#
pgcopydb list tables --source ${PGCOPYDB_SOURCE_PGURI}
pgcopydb list tables --source ${PGCOPYDB_TARGET_PGURI}

psql -a -1 ${PGCOPYDB_SOURCE_PGURI} <<EOF
create extension postgis cascade;
create extension postgis_topology cascade;
create extension pg_partman cascade;
EOF

psql -q -d ${PGCOPYDB_SOURCE_PGURI} -1 -f /usr/src/pagila/pagila-schema.sql
psql -q -d ${PGCOPYDB_SOURCE_PGURI} -1 -f /usr/src/pagila/pagila-data.sql
psql -q -d ${PGCOPYDB_SOURCE_PGURI} -1 -f /usr/src/pgcopydb/countries.sql

coproc ( pgcopydb snapshot -vv )

sleep 1

# copy the extensions separately
pgcopydb copy extensions

# pgcopydb copy db uses the environment variables
pgcopydb copy-db --skip-extensions

kill -TERM ${COPROC_PID}
wait ${COPROC_PID}

pgcopydb list extensions --source ${PGCOPYDB_SOURCE_PGURI}
pgcopydb list extensions --source ${PGCOPYDB_TARGET_PGURI}
