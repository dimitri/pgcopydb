#! /bin/bash

set -x
set -e

# This script expects the following environment variable(s) to be set:
#
#  - PGCOPYDB_SOURCE_PGURI

# `pgcopydb list table-parts` will use the table-size cache populated in
# `pgcopydb.table_size` in '4-list-table-split.sql'.
#
# The values stored in pgcopydb.table_size cache may not accurate, but the
# pgcopydb commands will make decisions based on the available information in
# the cache.

DIR=/tmp/unit/split
OPTS="--not-consistent --split-tables-larger-than 10kB"

pgcopydb list schema --dir ${DIR} ${OPTS} >/dev/null

pgcopydb list table-parts --dir ${DIR} \
    --split-tables-larger-than "10 kB" 2>/dev/null
