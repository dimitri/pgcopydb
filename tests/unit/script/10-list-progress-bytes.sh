#! /bin/bash

set -x
set -e

# This script expects the following environment variables to be set:
#
#  - PGCOPYDB_SOURCE_PGURI

# Regression for the `list progress` byte totals query: it summed `bytes` from
# s_table, but table sizes live in s_table_size (since #684). The stray column
# made SQLite fail to prepare the statement, so `list progress --json` aborted
# with no output instead of reporting a numeric bytes.total.

WORKDIR=/tmp/list-progress-bytes

# 1. Populate the catalog (schema/source.db) from the real source.
pgcopydb list tables --dir ${WORKDIR} > /dev/null 2>&1

# 2. list progress must succeed and expose a numeric bytes total. jq reduces
#    the (size-dependent) number to a stable sentinel so the diff is portable.
pgcopydb list progress --json --dir ${WORKDIR} 2>/dev/null \
  | jq -r 'if (.bytes.total | type) == "number" then "bytes.total is numeric" else "bytes.total missing" end'
