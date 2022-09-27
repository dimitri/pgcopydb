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
pgcopydb list tables --source ${POSTGRES_SOURCE}
pgcopydb list tables --source ${POSTGRES_TARGET}

psql -a ${POSTGRES_SOURCE} <<EOF
create role pagila NOSUPERUSER CREATEDB NOCREATEROLE LOGIN PASSWORD '0wn3d';
create database pagila owner pagila;
EOF

# copying roles needs superuser
# and we use the postgres database here still
pgcopydb copy roles --source ${POSTGRES_SOURCE} --target ${POSTGRES_TARGET}

# now create the pagila database on the target, owned by new role pagila
psql ${POSTGRES_TARGET} <<EOF
create database pagila owner pagila;
EOF

# create extensions on the source pagila database (needs superuser)
psql -a -1 ${PGCOPYDB_SOURCE_PGURI_SU} <<EOF
create extension postgis cascade;
create extension postgis_topology cascade;
EOF

# the partman extension needs to be installed as the pagila role
psql -a -1 ${PGCOPYDB_SOURCE_PGURI} <<EOF
create extension pg_partman cascade;
EOF

# create the application schema and data in the pagila database, role pagila
grep -v "OWNER TO postgres" /usr/src/pagila/pagila-schema.sql > /tmp/pagila-schema.sql

psql -q -d ${PGCOPYDB_SOURCE_PGURI} -1 -f /tmp/pagila-schema.sql
psql -q -d ${PGCOPYDB_SOURCE_PGURI} -1 -f /usr/src/pagila/pagila-data.sql
psql -q -d ${PGCOPYDB_SOURCE_PGURI} -1 -f /usr/src/pgcopydb/countries.sql

# take a snapshot using role pagila on source database
coproc ( pgcopydb snapshot --debug )

sleep 1

# copy the extensions separately, needs superuser (both on source and target)
pgcopydb copy extensions \
         --source ${PGCOPYDB_SOURCE_PGURI_SU} \
         --target ${PGCOPYDB_TARGET_PGURI_SU}

# now clone without superuser privileges (using role pagila on source and target)
pgcopydb clone --skip-extensions

kill -TERM ${COPROC_PID}
wait ${COPROC_PID}

pgcopydb list extensions --source ${PGCOPYDB_SOURCE_PGURI}
pgcopydb list extensions --source ${PGCOPYDB_TARGET_PGURI}
