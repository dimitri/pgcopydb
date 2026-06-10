#! /bin/bash

set -x
set -e

# This script expects the following environment variables to be set:
#
#  - PGCOPYDB_SOURCE_PGURI
#  - PGCOPYDB_TARGET_PGURI
#
# Regression test for https://github.com/dimitri/pgcopydb/issues/808
#
# Tables with both a PRIMARY KEY and an EXCLUSION constraint triggered
# "ERROR: index is already associated with a constraint" with --index-jobs 1.
# The bug was that EXCLUSION constraint indexes were excluded from the
# "indexes remaining" count, causing copydb_create_constraints to be called
# twice by the same worker when it processed the skipped EXCLUSION index.

TARGET_BASE="${PGCOPYDB_TARGET_PGURI%/*}"

for jobs in 1 4
do
    TMPDB="pgcopydb_bug808_ij${jobs}"
    TARGET_URI="${TARGET_BASE}/${TMPDB}"
    TMPDIR="/tmp/pgcopydb-bug808-ij${jobs}"

    psql -q -d "${PGCOPYDB_TARGET_PGURI}" -c "CREATE DATABASE ${TMPDB}"

    # The source schema contains a table using a custom collation.
    # Pre-create it on the target so that --skip-collations works correctly.
    psql -q -d "${TARGET_URI}" -c "
        CREATE COLLATION IF NOT EXISTS mycol
            (locale = 'fr-FR-x-icu', provider = 'icu');
    "

    pgcopydb clone \
        --source "${PGCOPYDB_SOURCE_PGURI}" \
        --target "${TARGET_URI}" \
        --index-jobs "${jobs}" \
        --table-jobs 2 \
        --dir "${TMPDIR}" \
        --not-consistent \
        --skip-collations \
        --fail-fast > /dev/null

    psql -d "${TARGET_URI}" \
         --no-psqlrc \
         --tuples-only \
         --no-align \
         -c "select c.conname || ' ' || c.contype::text
               from pg_constraint c
                    join pg_class r on r.oid = c.conrelid
              where r.relname = 'excl_with_pkey'
           order by c.conname"

    psql -q -d "${PGCOPYDB_TARGET_PGURI}" -c "DROP DATABASE ${TMPDB}"

    rm -rf "${TMPDIR}"
done
