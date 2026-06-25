#! /bin/bash
#
# Regression test for issues #501 and #484.
#
# This test reproduces the failure where REFRESH MATERIALIZED VIEW
# during pg_restore's post-data phase fails with:
#
#   ERROR: relation "documents" does not exist
#
# because pg_restore runs REFRESH with an empty search_path, but the
# matview's ts_stat() query string embeds an unqualified table name that
# only resolves when search_path includes the mvtest schema.
#
# The test also exercises dependency ordering and parallelism:
#
#   public.mv_word_stats      (Root 1, ts_stat search_path bug)
#   mvtest.mv_author_summary  (Root 2, independent)
#   mvtest.mv_tag_stats       (Root 3, parallel to roots 1+2)
#         |
#         +-- Root 1 + Root 2 --> mvtest.mv_combined  (Level 2)
#                                          |
#                                          +--> mvtest.mv_final  (Level 3)
#
# CURRENT STATUS: this test FAILS on unpatched pgcopydb because
# pgcopydb delegates REFRESH to pg_restore, which does not restore
# the correct search_path.  The fix (issue #501) is to have pgcopydb
# run REFRESH MATERIALIZED VIEW directly, with the schema-qualified
# name and the right search_path, bypassing pg_restore for this step.

set -x
set -e

# This script expects the following environment variables to be set:
#
#  - PGCOPYDB_SOURCE_PGURI
#  - PGCOPYDB_TARGET_PGURI
#  - PGCOPYDB_TABLE_JOBS
#  - PGCOPYDB_INDEX_JOBS

pgcopydb ping

# Load the matview tree (5 matviews across 4 levels) into source.
psql -a -d "${PGCOPYDB_SOURCE_PGURI}" -1 -f /usr/src/pgcopydb/setup.sql

# Verify source has the expected row counts before clone.
check_count() {
    local label=$1 uri=$2 query=$3 expected=$4
    local got
    got=$(psql -t -A -d "${uri}" -c "${query}")
    if [ "${got}" -ne "${expected}" ]; then
        echo "FAIL ${label}: expected ${expected} rows, got ${got}"
        exit 1
    fi
    echo "OK   ${label}: ${got} rows"
}

check_nonempty() {
    local label=$1 uri=$2 query=$3
    local got
    got=$(psql -t -A -d "${uri}" -c "${query}")
    if [ "${got}" -le 0 ]; then
        echo "FAIL ${label}: expected > 0 rows, got ${got}"
        exit 1
    fi
    echo "OK   ${label}: ${got} rows"
}

check_nonempty "source mv_word_stats"  "${PGCOPYDB_SOURCE_PGURI}" \
    "SELECT count(*) FROM public.mv_word_stats"
check_count "source mv_author_summary" "${PGCOPYDB_SOURCE_PGURI}" \
    "SELECT count(*) FROM mvtest.mv_author_summary" 3
check_count "source mv_tag_stats"      "${PGCOPYDB_SOURCE_PGURI}" \
    "SELECT count(*) FROM mvtest.mv_tag_stats"      3
check_nonempty "source mv_combined"    "${PGCOPYDB_SOURCE_PGURI}" \
    "SELECT count(*) FROM mvtest.mv_combined"
check_nonempty "source mv_final"       "${PGCOPYDB_SOURCE_PGURI}" \
    "SELECT count(*) FROM mvtest.mv_final"

# Clone source -> target.
# On unpatched pgcopydb this exits non-zero because pg_restore's REFRESH
# for public.mv_word_stats fails: the unqualified "documents" name in the
# ts_stat() query string is not found with an empty search_path.
pgcopydb clone --fail-fast --debug

# Verify all matviews were refreshed and contain data on the target.
# If clone succeeded, each view must have the same row count as the source.

target_word_count=$(psql -t -A -d "${PGCOPYDB_SOURCE_PGURI}" \
    -c "SELECT count(*) FROM public.mv_word_stats")
target_author_count=3
target_tag_count=3
target_combined_count=$(psql -t -A -d "${PGCOPYDB_SOURCE_PGURI}" \
    -c "SELECT count(*) FROM mvtest.mv_combined")
target_final_count=$(psql -t -A -d "${PGCOPYDB_SOURCE_PGURI}" \
    -c "SELECT count(*) FROM mvtest.mv_final")

check_count "target mv_word_stats"     "${PGCOPYDB_TARGET_PGURI}" \
    "SELECT count(*) FROM public.mv_word_stats"     "${target_word_count}"
check_count "target mv_author_summary" "${PGCOPYDB_TARGET_PGURI}" \
    "SELECT count(*) FROM mvtest.mv_author_summary" "${target_author_count}"
check_count "target mv_tag_stats"      "${PGCOPYDB_TARGET_PGURI}" \
    "SELECT count(*) FROM mvtest.mv_tag_stats"      "${target_tag_count}"
check_count "target mv_combined"       "${PGCOPYDB_TARGET_PGURI}" \
    "SELECT count(*) FROM mvtest.mv_combined"       "${target_combined_count}"
check_count "target mv_final"          "${PGCOPYDB_TARGET_PGURI}" \
    "SELECT count(*) FROM mvtest.mv_final"          "${target_final_count}"

echo ""
echo "matview-refresh: all checks PASSED"
