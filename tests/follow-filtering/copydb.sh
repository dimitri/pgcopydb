#! /bin/bash

set -x
set -e

# This script expects the following environment variables to be set:
#
#  - PGCOPYDB_SOURCE_PGURI
#  - PGCOPYDB_TARGET_PGURI
#  - PGCOPYDB_TABLE_JOBS
#  - PGCOPYDB_INDEX_JOBS

# Wait for source and target to be ready
pgcopydb ping

psql -o /tmp/s.out -d ${PGCOPYDB_SOURCE_PGURI} -1 -f /usr/src/pagila/pagila-schema.sql
psql -o /tmp/d.out -d ${PGCOPYDB_SOURCE_PGURI} -1 -f /usr/src/pagila/pagila-data.sql

# Set replica identity on partitioned payment tables
psql -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/ddl.sql

# Clone with pgoutput plugin and filtering (excludes public.staff).
# The publication will be created FOR TABLE ... without staff (server-side).
# The TCP coordinator is exposed so the inject container can set endpos.
pgcopydb clone \
    --follow \
    --plugin pgoutput \
    --filters /usr/src/pgcopydb/filters.ini \
    --host 0.0.0.0 \
    --port 5442 \
    --notice

#
# Verify that the filter excluded 'staff' from the initial clone.
#
staff_on_target=$(psql -AtqX -d ${PGCOPYDB_TARGET_PGURI} \
  -c "select count(*) from information_schema.tables
      where table_schema='public' and table_name='staff'")
echo "staff table present on target (should be 0): ${staff_on_target}"
test "${staff_on_target}" -eq 0

#
# Verify that the publication was created WITHOUT the staff table.
# This is the key assertion that proves server-side (publication) filtering.
#
pub_staff=$(psql -AtqX -d ${PGCOPYDB_SOURCE_PGURI} \
  -c "select count(*) from pg_publication_tables
      where pubname = 'pgcopydb' and tablename = 'staff'")
echo "staff in publication (should be 0): ${pub_staff}"
test "${pub_staff}" -eq 0

pub_actor=$(psql -AtqX -d ${PGCOPYDB_SOURCE_PGURI} \
  -c "select count(*) from pg_publication_tables
      where pubname = 'pgcopydb' and tablename = 'actor'")
echo "actor in publication (should be > 0): ${pub_actor}"
test "${pub_actor}" -gt 0

#
# Row count sanity check for several included tables.
#
for tbl in actor film category address; do
    src_count=$(psql -AtqX -d ${PGCOPYDB_SOURCE_PGURI} -c "select count(*) from ${tbl}")
    tgt_count=$(psql -AtqX -d ${PGCOPYDB_TARGET_PGURI} -c "select count(*) from ${tbl}")
    echo "source ${tbl}: ${src_count}, target ${tbl}: ${tgt_count}"
    test "${src_count}" -eq "${tgt_count}"
done

#
# Verify that the actor injected by dml.sql was replicated.
#
actor_on_target=$(psql -AtqX -d ${PGCOPYDB_TARGET_PGURI} \
  -c "select count(*) from actor where actor_id = 999")
echo "injected actor on target (should be 1): ${actor_on_target}"
test "${actor_on_target}" -eq 1

# Drop the publication created by pgcopydb
pgcopydb stream cleanup
