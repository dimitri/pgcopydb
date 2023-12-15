#! /bin/bash

set -x
set -e

# This script expects the following environment variables to be set:
#
#  - PGCOPYDB_SOURCE_PGURI

pgcopydb list collations -q --dir /tmp/collations 2>&1
