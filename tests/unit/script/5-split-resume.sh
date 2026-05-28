#! /bin/bash

set -x
set -e

# Regression test for the split-table TRUNCATE-on-resume bug.
#
# Before the fix, copydb_copy_supervisor_add_table_hook (table-data.c)
# unconditionally issued TRUNCATE on every partitioned table on every
# supervisor invocation, including --resume runs. Per-part workers then
# saw doneTime > 0 in the summary catalog and skipped the COPY — leaving
# the table empty.
#
# This script reproduces the data-loss path:
#   1. seed a fresh target table
#   2. run `pgcopydb copy table-data` with split-tables enabled (parts done)
#   3. drop a marker row into target — it exists only on target, not source
#   4. re-run with --resume
#   5. with the fix: TRUNCATE is gated on no parts being done, the marker
#      survives, and the original split-copied rows are preserved.

DIR=/tmp/unit/split-resume
TBL=public.table_1
FILTERS="$DIR/include-table_1.ini"

rm -rf "$DIR"
mkdir -p "$DIR"
cat > "$FILTERS" <<EOF
[include-only-table]
${TBL}
EOF

# Schema is already present on target from the unit suite's `pgcopydb fork`,
# but data was copied as a whole table (not split). Reset the target table so
# we can drive the split-copy + resume path explicitly.
psql -q -d "${PGCOPYDB_TARGET_PGURI}" -c "TRUNCATE ${TBL}" >/dev/null

# First run: copy table-data with a 10kB split threshold. table_1 holds
# 100 rows of char(580) — well over the threshold, so it splits.
PGCOPYDB_SPLIT_TABLES_LARGER_THAN=10kB \
    pgcopydb copy table-data \
        --dir "$DIR" \
        --not-consistent \
        --filters "$FILTERS" \
        --table-jobs 2

SRC_COUNT=$(psql -tA -d "${PGCOPYDB_SOURCE_PGURI}" -c "SELECT count(*) FROM ${TBL}")
TGT_COUNT=$(psql -tA -d "${PGCOPYDB_TARGET_PGURI}" -c "SELECT count(*) FROM ${TBL}")
echo "source-rows: ${SRC_COUNT}"
echo "target-rows-after-first-run: ${TGT_COUNT}"

# Drop a marker row that does NOT exist on the source — if --resume issues
# TRUNCATE, this marker disappears along with the originally-copied rows.
psql -q -d "${PGCOPYDB_TARGET_PGURI}" \
    -c "INSERT INTO ${TBL}(c_char) VALUES ('marker-row')" >/dev/null

# Second run with --resume: the per-part summary entries from the first run
# mean every part is already done. With the fix the supervisor skips the
# table-level TRUNCATE; without the fix it would wipe the table.
PGCOPYDB_SPLIT_TABLES_LARGER_THAN=10kB \
    pgcopydb copy table-data \
        --dir "$DIR" \
        --resume \
        --not-consistent \
        --filters "$FILTERS" \
        --table-jobs 2

# The marker row's survival is the proof that no TRUNCATE happened.
MARKER=$(psql -tA -d "${PGCOPYDB_TARGET_PGURI}" \
    -c "SELECT count(*) FROM ${TBL} WHERE c_char = 'marker-row'")
TGT_COUNT_AFTER=$(psql -tA -d "${PGCOPYDB_TARGET_PGURI}" \
    -c "SELECT count(*) FROM ${TBL}")
echo "marker-survived-resume: ${MARKER}"
echo "target-rows-after-resume: ${TGT_COUNT_AFTER}"
