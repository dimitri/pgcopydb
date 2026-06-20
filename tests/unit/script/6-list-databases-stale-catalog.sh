#! /bin/bash

set -x
set -e

# This script expects the following environment variables to be set:
#
#  - PGCOPYDB_SOURCE_PGURI

# Simulate issue #897: a source.db left over from a previous run against a
# different source URI must not block `pgcopydb list databases`.
#
# Steps:
#  1. Run `pgcopydb list tables` against the real source to populate source.db.
#  2. Replace the registered source URI in the catalog with a fake one.
#  3. Run `pgcopydb list databases` — it must succeed despite the mismatch.

WORKDIR=/tmp/list-databases-stale-catalog
SOURCEDB=${WORKDIR}/schema/source.db

# 1. Populate a work directory using the real source URI.
pgcopydb list tables --dir ${WORKDIR} > /dev/null 2>&1 || true

# 2. Overwrite the stored source URI with a fake server address.
sqlite3 -init /dev/null ${SOURCEDB} \
  "UPDATE setup SET source_pg_uri = 'postgres://fake-host/fakedb' WHERE id = 1;"

# 3. list databases with the real source URI must succeed even though source.db
#    was registered for a different one.
pgcopydb list databases --dir ${WORKDIR} 2>/dev/null \
  | awk '/postgres/ { print "found postgres database"; exit }'
