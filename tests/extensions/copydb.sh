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
pgcopydb ping --source ${POSTGRES_SOURCE} --target ${POSTGRES_TARGET}

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
create extension intarray cascade;
create extension postgis cascade;
create schema foo;
create extension hstore with schema foo cascade;
EOF

# create schemas for extensions on the target pagila database (needs superuser)
psql -a -1 ${PGCOPYDB_TARGET_PGURI_SU} <<EOF
create schema foo;
EOF

#
# create extension pg_partman cascade;
# create extension postgis_tiger_geocoder cascade;
#
# At the moment we don't have full support for pg_partman or
# postgis_tiger_geocoder without being superuser, because of a pg_dump
# limitation when it comes to extensions.
#
# pg_dump: error: query failed: ERROR:  permission denied for schema tiger
# pg_dump: error: query was: LOCK TABLE tiger.geocode_settings IN ACCESS SHARE MODE
#

# create the application schema and data in the pagila database, role pagila
grep -v "OWNER TO postgres" /usr/src/pagila/pagila-schema.sql > /tmp/pagila-schema.sql

psql -o /tmp/s.out -d ${PGCOPYDB_SOURCE_PGURI} -1 -f /tmp/pagila-schema.sql
psql -o /tmp/d.out -d ${PGCOPYDB_SOURCE_PGURI} -1 -f /usr/src/pagila/pagila-data.sql
psql -o /tmp/c.out -d ${PGCOPYDB_SOURCE_PGURI} -1 -f /usr/src/pgcopydb/countries.sql
psql -o /tmp/d.out -d ${PGCOPYDB_SOURCE_PGURI} -1 -f /usr/src/pgcopydb/ddl.sql

# take a snapshot using role pagila on source database
coproc ( pgcopydb snapshot --debug )

sleep 1

# copy the extensions separately, needs superuser (both on source and target)
pgcopydb list extensions --source ${PGCOPYDB_SOURCE_PGURI_SU}

# now get the extension versions requirements from the target server
e=/tmp/extensions.json
r=/tmp/requirements.json

# make sure to fetch the target list of extensions in a separate directory
TARGET_OPTS="--dir /tmp/target/pgcopydb"
TARGET_OPTS="${TARGET_OPTS} --source ${PGCOPYDB_TARGET_PGURI}"

pgcopydb list extensions --requirements --json ${TARGET_OPTS} > ${e}

jq 'map(select(.name == "postgis" or .name == "address_standardizer" or .name == "address_standardizer_data_us" or .name == "postgis_tiger_geocoder" or .name == "postgis_topology"))' < ${e} > ${r}

cat ${r}

pgcopydb copy extensions \
         --source ${PGCOPYDB_SOURCE_PGURI_SU} \
         --target ${PGCOPYDB_TARGET_PGURI_SU} \
         --requirements ${r} \
         --resume --debug

# now clone without superuser privileges (using role pagila on source and target)
pgcopydb clone --skip-extensions --restart

kill -TERM ${COPROC_PID}
wait ${COPROC_PID}

pgcopydb compare schema
pgcopydb compare data

pgcopydb list extensions --source ${PGCOPYDB_SOURCE_PGURI} --dir /tmp/check/source --debug
pgcopydb list extensions --source ${PGCOPYDB_TARGET_PGURI} --dir /tmp/check/target
