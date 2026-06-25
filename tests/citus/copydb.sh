#! /bin/bash

set -x
set -e

# This script expects the following environment variables to be set:
#
#  - PGCOPYDB_SOURCE_PGURI
#  - PGCOPYDB_TARGET_PGURI

# Wait for both Citus clusters to be ready (extension is pre-installed by the
# citusdata/citus Docker image entrypoint).
pgcopydb ping

# Install the Citus-native multi-tenant tutorial schema on source, then seed it
# with data.  The DDL script itself calls create_distributed_table() and
# create_reference_table() so the source is fully distributed before we clone.
psql -d "${PGCOPYDB_SOURCE_PGURI}" -f /usr/src/pgcopydb/ddl.sql
psql -d "${PGCOPYDB_SOURCE_PGURI}" -f /usr/src/pgcopydb/dml.sql

# Clone source → target.  pgcopydb detects the citus extension in the source
# catalog and calls create_distributed_table() / create_reference_table() on
# the target (in copydb_prepare_extensions_restore) before copying any data,
# so data is inserted through the Citus coordinator and lands in the correct
# shards.
pgcopydb clone --notice

# Verify that the distribution metadata matches on both sides.
psql -d "${PGCOPYDB_SOURCE_PGURI}" \
     -c "\copy (select table_name::text, citus_table_type::text, distribution_column \
                from citus_tables \
                order by table_name::text) \
         to '/tmp/source_citus_tables.out'"

psql -d "${PGCOPYDB_TARGET_PGURI}" \
     -c "\copy (select table_name::text, citus_table_type::text, distribution_column \
                from citus_tables \
                order by table_name::text) \
         to '/tmp/target_citus_tables.out'"

diff /tmp/source_citus_tables.out /tmp/target_citus_tables.out

echo "Distribution metadata matches between source and target"

# Verify row counts for every table.
for table in companies campaigns ads clicks impressions geo_ips; do
    src_count=$(psql -d "${PGCOPYDB_SOURCE_PGURI}" -t -c "select count(*) from ${table}")
    tgt_count=$(psql -d "${PGCOPYDB_TARGET_PGURI}" -t -c "select count(*) from ${table}")

    # trim whitespace
    src_count=$(echo "${src_count}" | tr -d ' ')
    tgt_count=$(echo "${tgt_count}" | tr -d ' ')

    echo "Table ${table}: source=${src_count} target=${tgt_count}"
    test "${src_count}" -eq "${tgt_count}"
done

echo "Row counts match for all tables"

# Verify schema parity (compare schemas on source and target).
pgcopydb compare schema
