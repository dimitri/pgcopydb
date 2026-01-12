#! /bin/bash

set -x
set -e

# This script expects the following environment variables to be set:
#
#  - PGCOPYDB_SOURCE_PGURI

# Strip OID column (first column) from output since OIDs vary between databases
pgcopydb list collations -q --dir /tmp/collations 2>&1 | \
    awk -F'|' 'NR<=2 {print $2"|"$3} NR>2 {print $2"|"$3}'
