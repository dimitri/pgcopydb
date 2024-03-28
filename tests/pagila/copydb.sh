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

# Create extensions in the default and alternative schemas to always
# ensure these copy as expected.
psql -d ${PAGILA_SOURCE_PGURI} <<EOF
create extension ltree;
create extension hstore;
create schema foo;
create extension intarray schema foo cascade;
EOF

grep -v "OWNER TO postgres" /usr/src/pagila/pagila-schema.sql > /tmp/pagila-schema.sql

psql -o /tmp/s.out -d ${PAGILA_SOURCE_PGURI} -1 -f /tmp/pagila-schema.sql
psql -o /tmp/d.out -d ${PAGILA_SOURCE_PGURI} -1 -f /usr/src/pagila/pagila-data.sql

function compare_schema() {
    pgcopydb compare schema \
             --source ${PAGILA_SOURCE_PGURI} \
             --target ${PAGILA_TARGET_PGURI}
}

function compare_data() {
    pgcopydb compare data \
             --source ${PAGILA_SOURCE_PGURI} \
             --target ${PAGILA_TARGET_PGURI}
}

pgcopydb clone --skip-ext-comments --notice \
         --source ${PAGILA_SOURCE_PGURI} \
         --target ${PAGILA_TARGET_PGURI}
compare_schema
compare_data

# Repeat the process with --drop-if-exists to ensure it is idempotent
pgcopydb clone --skip-ext-comments --notice \
         --source ${PAGILA_SOURCE_PGURI} \
         --target ${PAGILA_TARGET_PGURI} \
         --drop-if-exists
compare_schema
compare_data
