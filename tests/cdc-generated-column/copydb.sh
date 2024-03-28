#! /bin/bash

set -x
set -e
set -o pipefail

# This script expects the following environment variables to be set:
#
#  - PGCOPYDB_SOURCE_PGURI
#  - PGCOPYDB_TARGET_PGURI


for plugin in wal2json test_decoding; do

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
pgcopydb stream setup

# add some data to generated_column_test table
psql -d ${PGCOPYDB_SOURCE_PGURI} << EOF
begin;
insert into generated_column_test(id, name, email) values
(1, 'Tiger', 'tiger@wild.com'),
(2, 'Elephant', 'elephant@wild.com'),
(3, 'Cat', 'cat@home.net');
commit;

begin;
update generated_column_test set name = 'Lion'
where id = 1;
update generated_column_test set email='lion@wild.com'
where email = 'tiger@wild.com';
commit;

begin;
update generated_column_test set name = 'Kitten', email='kitten@home.com'
where id = 3;
commit;

begin;
delete from generated_column_test where id = 2;
commit;

EOF

pgcopydb stream sentinel set endpos --current
pgcopydb stream sentinel set apply
pgcopydb follow --notice --resume

sql="select * from generated_column_test"
psql -d ${PGCOPYDB_SOURCE_PGURI} -c "${sql}" > /tmp/s.out
psql -d ${PGCOPYDB_TARGET_PGURI} -c "${sql}" > /tmp/t.out

diff /tmp/s.out /tmp/t.out

kill -TERM ${COPROC_PID}
wait ${COPROC_PID}

pgcopydb stream cleanup --verbose

sql="truncate generated_column_test"
psql -d ${PGCOPYDB_SOURCE_PGURI} -c "${sql}" > /tmp/s.out
psql -d ${PGCOPYDB_TARGET_PGURI} -c "${sql}" > /tmp/s.out

rm -fr /tmp/pgcopydb /var/lib/postgres/.local/share/pgcopydb
done
