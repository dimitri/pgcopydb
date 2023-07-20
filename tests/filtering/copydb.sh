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
psql -o /tmp/e.out -d ${PGCOPYDB_SOURCE_PGURI} -1 -f /usr/src/pgcopydb/extra.sql

# list the exclude filters now, and the computed dependencies
cat /usr/src/pgcopydb/exclude.ini

# list the dependencies of objects that are selected by the filters
pgcopydb list depends --filters /usr/src/pgcopydb/exclude.ini

# list the dependencies of objects that are NOT selected by the filters
pgcopydb list depends --filters /usr/src/pgcopydb/exclude.ini --list-skipped

# list the sequences that are selected
pgcopydb list sequences --filters /usr/src/pgcopydb/exclude.ini

# list the sequences that are NOT selected
pgcopydb list sequences --filters /usr/src/pgcopydb/exclude.ini --list-skipped

pgcopydb clone --filters /usr/src/pgcopydb/exclude.ini

# now another migration with the "include-only" parts of the data
pgcopydb clone --filters /usr/src/pgcopydb/include.ini --restart

# print out the definition of the copy.foo table
psql -d ${PGCOPYDB_SOURCE_PGURI} -c '\d app|copy.foo'
psql -d ${PGCOPYDB_TARGET_PGURI} -c '\d app|copy.foo'

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
    diff $e $r || cat $r
    diff $e $r || exit 1
done
