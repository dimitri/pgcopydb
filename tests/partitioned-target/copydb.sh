#! /bin/bash

set -x
set -e

# This script expects the following environment variables to be set:
#
#  - PGCOPYDB_SOURCE_PGURI
#  - PGCOPYDB_TARGET_PGURI
#  - PGCOPYDB_TABLE_JOBS

# make sure source and target databases are ready
pgcopydb ping

# build the asymmetric fixture: flat on source, partitioned on target
psql -a -d "${PGCOPYDB_SOURCE_PGURI}" -1 -f /usr/src/pgcopydb/source.sql
psql -a -d "${PGCOPYDB_TARGET_PGURI}" -1 -f /usr/src/pgcopydb/target.sql

# copy data only: target's partitioned schema must be preserved.
# this would previously fail with:
#   ERROR: cannot truncate only a partitioned table
#   ERROR: cannot perform COPY FREEZE on a partitioned table
pgcopydb copy table-data --not-consistent

# verify row counts match between source and target
src_count=$(psql -At -d "${PGCOPYDB_SOURCE_PGURI}" \
                 -c 'select count(*) from partitioned_target.events')
dst_count=$(psql -At -d "${PGCOPYDB_TARGET_PGURI}" \
                 -c 'select count(*) from partitioned_target.events')

echo "source rows: ${src_count}, target rows: ${dst_count}"

if [ "${src_count}" != "${dst_count}" ]; then
    echo "row count mismatch"
    exit 1
fi

# verify the rows actually landed in the right partitions on target
psql -d "${PGCOPYDB_TARGET_PGURI}" \
     -c 'select bucket, count(*) from partitioned_target.events group by bucket order by bucket'
