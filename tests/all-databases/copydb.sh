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
# Load pagila database on source
#
createdb -h source -U postgres pagila
psql -o /tmp/pagila-schema.out -h source -U postgres -d pagila \
     -1 -f /usr/src/pagila/pagila-schema.sql
psql -o /tmp/pagila-data.out -h source -U postgres -d pagila \
     -1 -f /usr/src/pagila/pagila-data.sql

#
# Load f1db database on source (pg_restore from custom-format dump)
#
createdb -h source -U postgres f1db
pg_restore -h source -U postgres -d f1db --no-owner --no-privileges \
           /usr/src/f1db/f1db.dump

#
# Load chinook database on source (the SQL file creates its own database)
#
psql -h source -U postgres -f /usr/src/chinook/Chinook_PostgreSql.sql

#
# Clone all databases from source to target using --all-databases
#
pgcopydb clone --all-databases \
         --notice \
         --skip-collations \
         --skip-large-objects \
         --source "${PGCOPYDB_SOURCE_PGURI}" \
         --target "${PGCOPYDB_TARGET_PGURI}"

pgcopydb compare schema \
         --all-databases \
         --source "${PGCOPYDB_SOURCE_PGURI}" \
         --target "${PGCOPYDB_TARGET_PGURI}" \
         --dir "/tmp/pgcopydb"

pgcopydb compare data \
         --all-databases \
         --source "${PGCOPYDB_SOURCE_PGURI}" \
         --target "${PGCOPYDB_TARGET_PGURI}" \
         --dir "/tmp/pgcopydb"
