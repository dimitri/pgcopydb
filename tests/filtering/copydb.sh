#! /bin/bash

set -x
set -e

# This script expects the following environment variables to be set:
#
#  - PGCOPYDB_SOURCE_PGURI
#  - PGCOPYDB_TARGET_PGURI
#  - PGCOPYDB_TABLE_JOBS
#  - PGCOPYDB_INDEX_JOBS

# make sure source and target databases are ready
pgcopydb ping

psql -o /tmp/s.out -d ${PGCOPYDB_SOURCE_PGURI} -1 -f /usr/src/pagila/pagila-schema.sql
psql -o /tmp/d.out -d ${PGCOPYDB_SOURCE_PGURI} -1 -f /usr/src/pagila/pagila-data.sql

# pgcopydb clone uses the environment variables
pgcopydb clone --filters /usr/src/pgcopydb/include.ini

# now compare the output of running the SQL command with what's expected
# as we're not root when running tests, can't write in /usr/src
mkdir -p /tmp/results

find .

pgopts="--single-transaction --no-psqlrc --expanded"

for f in ./sql/*.sql
do
    t=`basename $f .sql`
    r=/tmp/results/${t}.out
    e=./expected/${t}.out
    psql -d "${PGCOPYDB_TARGET_PGURI}" ${pgopts} --file ./sql/$t.sql &> $r
    test -f $e || cat $r
    diff $e $r || exit 1
done
