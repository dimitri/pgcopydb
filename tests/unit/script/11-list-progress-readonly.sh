#! /bin/bash

set -x
set -e

# This script expects the following environment variables to be set:
#
#  - PGCOPYDB_SOURCE_PGURI

# Regression for `list progress` opening the source catalog read-write, which
# contended with a concurrently running clone and surfaced as "[SQLite 5:
# database is locked]". A read-only open must poll without a write lock.

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
