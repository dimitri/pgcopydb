#! /bin/bash

set -x
set -e
set -o pipefail

# cdc-transform-apply: unit test for the stream transform + apply pipeline.
#
# This test requires NO live Postgres connections.  The CDC messages are
# pre-canned JSON-lines fixture files and the source schema is pre-built.
# The test exercises:
#
#   1. pgcopydb stream init        -- initialises the work dir and SQLite DBs.
#   2. Seed source.db              -- load source_schema.sql into the SQLite
#                                     catalog (table + column attributes).
#   3. pgcopydb stream receive     -- --from-file: loads fixture rows into the
#      --from-file <fixture.jsonl>    replayDB output table (no PG slot).
#   4. pgcopydb stream transform   -- reads output table → writes stmt+replay.
#      --target -                     (local identifier quoting, no PG needed)
#   5. Verify stmt table contents  -- via sqlite3, diff against expected file.
#   6. pgcopydb stream apply       -- --target -: reads stmt+replay → stdout.
#   7. Verify apply output         -- diff against expected SQL file.
#
# The cycle repeats for test_decoding and wal2json fixtures.

FIXTURES=/usr/src/pgcopydb
WORKDIR=/tmp/pgcopydb

# ---------------------------------------------------------------------------
# Helper: find the first replayDB SQLite file under the CDC work directory.
# ---------------------------------------------------------------------------
replaydb () {
    ls "${WORKDIR}/cdc/"*.db 2>/dev/null | head -1
}

# ---------------------------------------------------------------------------
# Helper: dump unique SQL templates ordered by first replay row, compare.
# ---------------------------------------------------------------------------
check_stmts () {
    local label="$1"
    local dbfile
    dbfile=$(replaydb)

    sqlite3 -init /dev/null -list -noheader "${dbfile}" \
        "select s.sql
           from stmt s
           join replay r on r.stmt_hash = s.hash
          where r.action not in ('B','C','R','K','x','E')
          group by s.hash
          order by min(r.id)" \
        > /tmp/actual_stmts.sql

    diff "${FIXTURES}/expected_stmts_${label}.sql" /tmp/actual_stmts.sql
    echo "OK: ${label}: stmt table matches expected"
}

# ---------------------------------------------------------------------------
# Helper: run apply to stdout and diff against the expected SQL file.
# ---------------------------------------------------------------------------
check_apply () {
    local label="$1"

    pgcopydb stream apply \
        --dir "${WORKDIR}" \
        --target - \
        --resume \
        --not-consistent \
        > /tmp/actual_apply_${label}.sql

    diff "${FIXTURES}/expected_apply_${label}.sql" /tmp/actual_apply_${label}.sql
    echo "OK: ${label}: apply SQL matches expected"
}

# ===========================================================================
# ROUND 1: test_decoding
# ===========================================================================
echo "=== ROUND 1: test_decoding ==="

# Step 1: initialise work dir (no --source needed; schema loaded from fixture).
pgcopydb stream init \
    --dir "${WORKDIR}" \
    --slot-name pgcopydb_unit \
    --plugin test_decoding \
    --restart

# Step 2: seed the source catalog with table + column attributes so the
# transform step can look up primary-key columns for UPDATE/DELETE.
SOURCEDB="${WORKDIR}/schema/source.db"
sqlite3 "${SOURCEDB}" ".read ${FIXTURES}/source_schema.sql"

# Step 3: load fixture rows into the replayDB output table.
pgcopydb stream receive \
    --from-file "${FIXTURES}/fixture_test_decoding.jsonl" \
    --dir "${WORKDIR}" \
    --slot-name pgcopydb_unit \
    --plugin test_decoding \
    --endpos 0/1519A60 \
    --not-consistent

echo "--- output table contents (test_decoding) ---"
sqlite3 "$(replaydb)" \
    "select id, action, xid, lsn, substr(message,1,80) from output order by id"

# Step 4: transform output → stmt + replay.
# --target - : use local identifier quoting; no Postgres connection needed.
pgcopydb stream transform \
    --dir "${WORKDIR}" \
    --target - \
    --slot-name pgcopydb_unit \
    --plugin test_decoding \
    --endpos 0/1519A60 \
    --resume \
    --not-consistent

echo "--- stmt table (test_decoding) ---"
sqlite3 "$(replaydb)" "select hash, substr(sql,1,80) from stmt order by hash"

echo "--- replay table (test_decoding) ---"
sqlite3 "$(replaydb)" \
    "select id, action, xid, lsn, stmt_hash, substr(stmt_args,1,60) from replay order by id"

# Step 5: verify stmt table.
check_stmts "test_decoding"

# Step 6+7: apply to stdout and verify.
check_apply "test_decoding"

# ===========================================================================
# ROUND 2: wal2json  (same data, different plugin)
# ===========================================================================
echo "=== ROUND 2: wal2json ==="

# Wipe the CDC replayDB so we start clean (keep schema source.db).
rm -f "${WORKDIR}"/cdc/*.db "${WORKDIR}"/cdc/*.db-shm "${WORKDIR}"/cdc/*.db-wal

pgcopydb stream init \
    --dir "${WORKDIR}" \
    --slot-name pgcopydb_unit \
    --plugin wal2json \
    --restart

sqlite3 "${SOURCEDB}" ".read ${FIXTURES}/source_schema.sql"

pgcopydb stream receive \
    --from-file "${FIXTURES}/fixture_wal2json.jsonl" \
    --dir "${WORKDIR}" \
    --slot-name pgcopydb_unit \
    --plugin wal2json \
    --endpos 0/1519A60 \
    --not-consistent

echo "--- output table contents (wal2json) ---"
sqlite3 "$(replaydb)" \
    "select id, action, xid, lsn, substr(message,1,80) from output order by id"

pgcopydb stream transform \
    --dir "${WORKDIR}" \
    --target - \
    --slot-name pgcopydb_unit \
    --plugin wal2json \
    --endpos 0/1519A60 \
    --resume \
    --not-consistent

check_stmts "wal2json"
check_apply "wal2json"

echo "=== All checks passed ==="
