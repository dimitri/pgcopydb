#! /bin/bash

set -x
set -e
set -o pipefail

# This script expects the following environment variables to be set:
#
#  - PGCOPYDB_SOURCE_PGURI
#  - PGCOPYDB_TARGET_PGURI


for plugin in test_decoding wal2json; do

# make sure source and target databases are ready
pgcopydb ping

# create the table on the source and target databases
psql -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/ddl.sql
psql -d ${PGCOPYDB_TARGET_PGURI} -f /usr/src/pgcopydb/ddl.sql

# create the replication slot that captures all the changes
# PGCOPYDB_OUTPUT_PLUGIN is set to test_decoding in docker-compose.yml
coproc ( pgcopydb snapshot --follow --plugin ${plugin} --verbose)

# wait for the snapshot to be created
while [ ! -f /tmp/pgcopydb/snapshot ]; do
  sleep 1
done

# wait for snapshot to be created

# now setup the replication origin (target) and the pgcopydb.sentinel (source)
pgcopydb stream setup --plugin ${plugin} --verbose

# add some data to generated_column_test table
psql -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/dml.sql

pgcopydb stream sentinel set endpos --current
pgcopydb stream sentinel set apply
pgcopydb follow --notice --resume

# count the number of rows in the generated_column_test table
# on the source and target databases
sql="select count(*) from generated_column_test"
src_count=`psql -AtqX -d ${PGCOPYDB_TARGET_PGURI} -c "${sql}"`
tgt_count=`psql -AtqX -d ${PGCOPYDB_SOURCE_PGURI} -c "${sql}"`
test ${src_count} -gt 0
test ${tgt_count} -gt 0
test ${src_count} -eq ${tgt_count}

sql="select * from generated_column_test"
psql -d ${PGCOPYDB_SOURCE_PGURI} -c "${sql}" > /tmp/s.out
psql -d ${PGCOPYDB_TARGET_PGURI} -c "${sql}" > /tmp/t.out

diff /tmp/s.out /tmp/t.out || cat /tmp/s.out /tmp/t.out

kill -TERM ${COPROC_PID}
wait ${COPROC_PID}

pgcopydb stream cleanup --verbose

# create a copy of the generated_column_test table on the target database
# so that we can retain the data for debugging and comparison.
sql="CREATE TABLE ${plugin}_generated_column_test AS SELECT * FROM generated_column_test"
psql -d ${PGCOPYDB_TARGET_PGURI} -c "${sql}" > /tmp/t.out

sql="truncate generated_column_test"
psql -d ${PGCOPYDB_SOURCE_PGURI} -c "${sql}" > /tmp/s.out
psql -d ${PGCOPYDB_TARGET_PGURI} -c "${sql}" > /tmp/t.out

rm -fr /tmp/pgcopydb /var/lib/postgres/.local/share/pgcopydb
done

# verify both wal2json and test_decoding outputs are the same.
sql="select * from wal2json_generated_column_test"
psql -d ${PGCOPYDB_TARGET_PGURI} -c "${sql}" > /tmp/a.out
sql="select * from test_decoding_generated_column_test"
psql -d ${PGCOPYDB_TARGET_PGURI} -c "${sql}" > /tmp/b.out

diff /tmp/a.out /tmp/b.out || cat /tmp/a.out /tmp/b.out
