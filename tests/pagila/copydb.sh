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
create schema bar;
create extension pg_trgm schema bar cascade;
create schema "Foo Bar";
create extension pgcrypto schema "Foo Bar" cascade;
create schema "With a space";
create extension seg schema "With a space" cascade;
EOF

# Create a couple of schemas in the target database
# to ensure they get skipped during the clone
psql -d ${PAGILA_TARGET_PGURI} <<EOF
create schema bar;
create schema "With a space";
EOF

grep -v "OWNER TO postgres" /usr/src/pagila/pagila-schema.sql > /tmp/pagila-schema.sql

psql -o /tmp/s.out -d ${PAGILA_SOURCE_PGURI} -1 -f /tmp/pagila-schema.sql
psql -o /tmp/d.out -d ${PAGILA_SOURCE_PGURI} -1 -f /usr/src/pagila/pagila-data.sql

pgcopydb clone --notice \
         --skip-ext-comments \
         --skip-db-properties \
         --estimate-table-sizes \
         --use-copy-binary \
         --source ${PAGILA_SOURCE_PGURI} \
         --target ${PAGILA_TARGET_PGURI}

pgcopydb compare schema \
         --source ${PAGILA_SOURCE_PGURI} \
         --target ${PAGILA_TARGET_PGURI}

pgcopydb compare data \
         --source ${PAGILA_SOURCE_PGURI} \
         --target ${PAGILA_TARGET_PGURI}
