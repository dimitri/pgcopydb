#! /bin/bash

set -x
set -e

# This script expects the following environment variables to be set:
#
#  - PGCOPYDB_SOURCE_PGURI
#  - PGCOPYDB_SOURCE_STANDBY_PGURI
#  - PGCOPYDB_TARGET_PGURI
#  - PGCOPYDB_TABLE_JOBS
#  - PGCOPYDB_INDEX_JOBS

# make sure source and target databases are ready
pgcopydb ping

# sleep 5 seconds to make sure standby is ready
sleep 5

grep -v "OWNER TO postgres" /usr/src/pagila/pagila-schema.sql > /tmp/pagila-schema.sql

psql -o /tmp/s.out -d ${PGCOPYDB_SOURCE_PGURI} -1 -f /tmp/pagila-schema.sql
psql -o /tmp/d.out -d ${PGCOPYDB_SOURCE_PGURI} -1 -f /usr/src/pagila/pagila-data.sql

# Demonstrate that filtering (including regex patterns) works from a standby
# source.  The ~/^payment_p/ regex matches the seven payment partition tables;
# they should appear in the "skipped" list and be absent from the selected list.
cat > /tmp/standby-filter.ini <<'INI'
[exclude-table]
public.~/^payment_p/
INI

pgcopydb list tables \
         --filters /tmp/standby-filter.ini \
         --source ${PGCOPYDB_SOURCE_STANDBY_PGURI}

pgcopydb list tables \
         --filters /tmp/standby-filter.ini \
         --source ${PGCOPYDB_SOURCE_STANDBY_PGURI} \
         --list-skipped

pgcopydb list sequences \
         --filters /tmp/standby-filter.ini \
         --source ${PGCOPYDB_SOURCE_STANDBY_PGURI}

pgcopydb clone --skip-ext-comments --notice \
         --source ${PGCOPYDB_SOURCE_STANDBY_PGURI} \
         --target ${PGCOPYDB_TARGET_PGURI}

pgcopydb compare schema \
         --source ${PGCOPYDB_SOURCE_STANDBY_PGURI} \
         --target ${PGCOPYDB_TARGET_PGURI}

pgcopydb compare data \
         --source ${PGCOPYDB_SOURCE_STANDBY_PGURI} \
         --target ${PGCOPYDB_TARGET_PGURI}
