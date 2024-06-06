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

# Cached size of table_1 is 100 KB, so this will be split into 10 parts
pgcopydb list table-parts --dir ${DIR} \
    --schema-name "public" --table-name "table_1" \
    --split-tables-larger-than "10 kB" 2>&1

# table_2 is identical to table_1 but with the size of 50 KB, so this will be
# split into 5 parts
pgcopydb list table-parts --dir ${DIR} \
    --schema-name "public" --table-name "table_2" \
    --split-tables-larger-than "10 kB" 2>&1

# table_3 doesn't have size in cache, therefore it will not be split
pgcopydb list table-parts --dir ${DIR} \
    --schema-name "public" --table-name "table_3" \
    --split-tables-larger-than "10 kB" 2>&1

# table_ctid_candidate doesn't have a unique integer, therefore it will be split by ctid
pgcopydb list table-parts --dir ${DIR} \
    --schema-name "public" --table-name "table_ctid_candidate" \
    --split-tables-larger-than "10 kB" 2>&1

# ctid split is disabled, no partitioning will be done
pgcopydb list table-parts --dir ${DIR} \
    --schema-name "public" --table-name "table_ctid_candidate_skip" \
    --split-tables-larger-than "10 kB" \
    --skip-split-by-ctid 2>&1


# Now we will test the limits on the number of parts
DIR=/tmp/unit/split-with-limits
OPTS+=" --split-max-parts 3"

pgcopydb list schema --dir ${DIR} ${OPTS} >/dev/null

# limits on the number of parts will be respected, and we will see 3 parts now
pgcopydb list table-parts --dir ${DIR} \
    --schema-name "public" --table-name "table_1" \
    --split-tables-larger-than "10 kB" --split-max-parts 3 2>&1
