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

export TMPDIR=/tmp/exclude

# list the exclude filters now, and the computed dependencies
cat /usr/src/pgcopydb/exclude.ini

# list the tables that are (not) selected by the filters
pgcopydb list tables --filters /usr/src/pgcopydb/exclude.ini
pgcopydb list tables --filters /usr/src/pgcopydb/exclude.ini --list-skipped

# list the dependencies of objects that are not selected by the filters
pgcopydb list depends --filters /usr/src/pgcopydb/exclude.ini --list-skipped

# list the sequences that are (not) selected by the filters
pgcopydb list sequences --filters /usr/src/pgcopydb/exclude.ini
pgcopydb list sequences --filters /usr/src/pgcopydb/exclude.ini --list-skipped

sqlite3 /tmp/exclude/pgcopydb/schema/filter.db <<EOF
select * from section;

select oid, qname, restore_list_name, ownedby, attrelid, attroid from s_seq;

select f.kind, count(*) from filter f group by kind;
select f.oid, f.kind from filter f where kind like 'sequence';

attach '/tmp/exclude/pgcopydb/schema/source.db' as source;

select NULL as oid, s.restore_list_name, 'sequence owned by'
  from s_seq s
 where not exists
       (select 1 from source.s_table st where st.oid = s.ownedby);
EOF

pgcopydb clone --filters /usr/src/pgcopydb/exclude.ini --resume --not-consistent

export TMPDIR=/tmp/include

# list the tables that are (not) selected by the filters
pgcopydb list tables --filters /usr/src/pgcopydb/include.ini
pgcopydb list tables --filters /usr/src/pgcopydb/include.ini --list-skipped

# now another migration with the "include-only" parts of the data
pgcopydb clone --filters /usr/src/pgcopydb/include.ini --resume --not-consistent

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

# --follow tests
pgcopydb stream cleanup

# --follow with inclusion filters
coproc (pgcopydb follow --plugin test_decoding --filters /usr/src/pgcopydb/include.ini --restart)

# execute DML queries against the source database to test filtering for include.ini
psql -d "${PGCOPYDB_SOURCE_PGURI}" ${pgopts} --file ./follow-mode/dml-for-include.sql

# make sure the inject service has had time to see the final sentinel values
sleep 2

pgcopydb stream sentinel get

# set the end position to the current position to complete the follow operation
pgcopydb stream sentinel set endpos --current --debug

# cleanup the stream
pgcopydb stream cleanup

# --follow with exclusion filters
coproc (pgcopydb follow --plugin test_decoding --filters /usr/src/pgcopydb/exclude.ini --restart --not-consistent)

# execute DML queries against the source database to test filtering for include.ini
psql -d "${PGCOPYDB_SOURCE_PGURI}" ${pgopts} --file ./follow-mode/dml-for-exclude.sql

# set the end position to the current position to complete the follow operation
pgcopydb stream sentinel set endpos --current

# make sure the inject service has had time to see the final sentinel values
sleep 2

# cleanup the stream
pgcopydb stream cleanup

# run assertions on the target database
for f in ./follow-mode/sql/*.sql
do
    t=`basename $f .sql`
    r=/tmp/results/${t}.out
    e=./expected/${t}.out
    psql -d "${PGCOPYDB_TARGET_PGURI}" ${pgopts} --file ./sql/$t.sql &> $r
    test -f $e || cat $r
    diff $e $r || cat $r
    diff $e $r || exit 1
done
