#! /bin/bash

set -x
set -e

# This script expects the following environment variables to be set:
#
#  - PGCOPYDB_SOURCE_PGURI

# Strip OID column (first column) from output since OIDs vary between databases.
# Use sed rather than cut: data rows use '|' as delimiter but the separator row
# uses '+', so cut would leave the separator with an extra leading column.
pgcopydb list collations -q --dir /tmp/collations 2>&1 | sed 's/^[^|+]*[|+]//'
