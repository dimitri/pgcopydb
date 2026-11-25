#! /bin/bash

set -x
set -e

# This script expects the following environment variables to be set:
#
#  - PGCOPYDB_SOURCE_PGURI
#  - PGCOPYDB_TARGET_PGURI
#  - PGCOPYDB_TABLE_JOBS
#
# What this test covers
# =====================
#
# A split table's id ranges start at min(id). That min must be read in the
# snapshot the COPY uses: rows deleted after the snapshot was exported are
# still visible to the COPY, and a min read later lands above them, so the
# first range skips them.

SEED_ROWS=50000
DELETED_ROWS=1000

# make sure source and target databases are ready
pgcopydb ping

psql -d ${PGCOPYDB_SOURCE_PGURI} -c "
  create table t(id bigint primary key, note text)"

psql -d ${PGCOPYDB_SOURCE_PGURI} -c "
  insert into t select g, repeat('x', 100) from generate_series(1,${SEED_ROWS}) g"

coproc ( psql -At -d ${PGCOPYDB_SOURCE_PGURI} 2>&1 )

echo 'begin;' >&"${COPROC[1]}"
read x <&"${COPROC[0]}"

echo 'set transaction isolation level repeatable read;' >&"${COPROC[1]}"
read x <&"${COPROC[0]}"

echo 'select pg_export_snapshot();' >&"${COPROC[1]}"
read sn <&"${COPROC[0]}"

# rows below every range bound, gone from the source but not from the snapshot
psql -d ${PGCOPYDB_SOURCE_PGURI} -c "delete from t where id <= ${DELETED_ROWS}"

pgcopydb clone --snapshot "${sn}" --split-tables-larger-than 1MB --notice

echo 'commit;' >&"${COPROC[1]}"

tgt=$(psql -AtqX -d ${PGCOPYDB_TARGET_PGURI} -c "select count(*) from t")
min=$(psql -AtqX -d ${PGCOPYDB_TARGET_PGURI} -c "select min(id) from t")

echo "target rows ${tgt}, min id ${min}"

test "${tgt}" -eq ${SEED_ROWS}
test "${min}" -eq 1
