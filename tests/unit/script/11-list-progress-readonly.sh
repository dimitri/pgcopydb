#! /bin/bash

set -x
set -e

# This script expects the following environment variables to be set:
#
#  - PGCOPYDB_SOURCE_PGURI

# Regression for `list progress` opening the source catalog read-write.
#
# `list progress` only reads the catalog, but it used to open it read-write
# (catalog_init_from_specs): a plain sqlite3_open that takes a write semaphore
# and checkpoints the WAL on close. Polling progress during a live clone then
# contended with the clone's writers and surfaced as "[SQLite 5: database is
# locked]" in the clone, which --fail-fast turned into a torn-down transfer.
#
# The fix opens the catalog read-only. Two observable guarantees prove it:
#
#  1. Cold catalog: when schema/source.db does not exist yet (very early in a
#     run), a read-only open cannot create it. The old read-write path created
#     the file as a side effect of sqlite3_open; the read-only path reports
#     zeroed progress and leaves the filesystem untouched. "source.db absent"
#     after the call is the race-free signal that the open was read-only.
#
#  2. Warm catalog: against a populated catalog, progress still reports real
#     numeric byte totals (the read path itself is unchanged).

WORKDIR=/tmp/list-progress-readonly

# 1. Cold catalog: the work dir exists (copydb refuses a missing one) but the
#    catalog file does not. Read-only open must report zeros and create nothing.
COLD=${WORKDIR}-cold
rm -rf ${COLD}
mkdir -p ${COLD}/schema

pgcopydb list progress --json --dir ${COLD} 2>/dev/null \
  | jq -r '"cold bytes.total: " + (.bytes.total | tostring)'

test -f ${COLD}/schema/source.db \
  && echo "cold catalog: source.db created (read-write open)" \
  || echo "cold catalog: source.db absent (read-only open)"

# 2. Warm catalog: populate it with `list tables`, then progress must still
#    expose a numeric bytes total. jq reduces the (size-dependent) number to a
#    stable sentinel so the diff is portable.
WARM=${WORKDIR}-warm
rm -rf ${WARM}
pgcopydb list tables --dir ${WARM} > /dev/null 2>&1

pgcopydb list progress --json --dir ${WARM} 2>/dev/null \
  | jq -r 'if (.bytes.total | type) == "number" then "warm catalog: bytes.total is numeric" else "warm catalog: bytes.total missing" end'
