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
# pgcopydb list extensions include a retry loop, so we use that as a proxy
# to depend on the source/target Postgres images to be ready
#
pgcopydb list extensions --source ${PGCOPYDB_SOURCE_PGURI}
pgcopydb list extensions --source ${PGCOPYDB_TARGET_PGURI}

#
# Now create a user that's going to be the owner of our database
#
psql -a ${PGCOPYDB_SOURCE_PGURI} <<EOF
create role pagila NOSUPERUSER CREATEDB NOCREATEROLE LOGIN PASSWORD '0wn3d';
create database pagila owner pagila connection limit 8;
EOF

pgcopydb copy roles

psql -a ${PGCOPYDB_TARGET_PGURI} <<EOF
create database pagila owner pagila connection limit 10;
EOF

PAGILA_SOURCE_PGURI="postgres://pagila:0wn3d@source/pagila"
PAGILA_TARGET_PGURI="postgres://pagila:0wn3d@target/pagila"

grep -v "OWNER TO postgres" /usr/src/pagila/pagila-schema.sql > /tmp/pagila-schema.sql

psql -o /tmp/s.out -d ${PAGILA_SOURCE_PGURI} -1 -f /tmp/pagila-schema.sql
psql -o /tmp/d.out -d ${PAGILA_SOURCE_PGURI} -1 -f /usr/src/pagila/pagila-data.sql

pgcopydb clone --source ${PAGILA_SOURCE_PGURI} --target ${PAGILA_TARGET_PGURI}
