#! /bin/bash

set -x
set -e

# This script exercises pgcopydb with three different databases cloned
# sequentially using a separate pgcopydb clone invocation per database.
# Each invocation gets its own --dir so the catalogs don't overlap.
#
# Environment variables expected:
#
#  - PGCOPYDB_SOURCE_PGURI   (instance-level URI, e.g. postgres://postgres@source/postgres)
#  - PGCOPYDB_TARGET_PGURI   (instance-level URI, e.g. postgres://postgres@target/postgres)
#  - PGCOPYDB_TABLE_JOBS
#  - PGCOPYDB_INDEX_JOBS

pgcopydb ping

#
# ─── Load pagila ─────────────────────────────────────────────────────────────
#
createdb -h source -U postgres pagila
psql -o /tmp/pagila-schema.out -h source -U postgres -d pagila \
     -1 -f /usr/src/pagila/pagila-schema.sql
psql -o /tmp/pagila-data.out   -h source -U postgres -d pagila \
     -1 -f /usr/src/pagila/pagila-data.sql

#
# ─── Load f1db ────────────────────────────────────────────────────────────────
#
createdb -h source -U postgres f1db
pg_restore -h source -U postgres -d f1db --no-owner --no-privileges \
           /usr/src/f1db/f1db.dump

#
# ─── Load chinook ─────────────────────────────────────────────────────────────
#
psql -h source -U postgres -f /usr/src/chinook/Chinook_PostgreSql.sql

#
# ─── Clone each database individually ────────────────────────────────────────
#
for db in pagila f1db chinook; do
    pgcopydb clone \
             --notice \
             --skip-collations \
             --skip-large-objects \
             --source "postgres://postgres@source/${db}" \
             --target "postgres://postgres@target/${db}" \
             --dir    "/tmp/pgcopydb-${db}"

    pgcopydb compare schema \
             --source "postgres://postgres@source/${db}" \
             --target "postgres://postgres@target/${db}" \
             --dir    "/tmp/pgcopydb-${db}"

    pgcopydb compare data \
             --source "postgres://postgres@source/${db}" \
             --target "postgres://postgres@target/${db}" \
             --dir    "/tmp/pgcopydb-${db}"
done
