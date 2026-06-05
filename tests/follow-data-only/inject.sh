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

#
# Only start injecting DML traffic on the source database when pgcopydb
# follow command has been started. We know that by querying the SQLite
# catalogs database, where the prefetch/transform/catchup sub-processes
# register themselves in the process table.
#
dbfile=$(find ${TMPDIR}/pgcopydb/schema -name "source.db" -type f 2>/dev/null | head -1)

until [ -n "${dbfile}" ] && [ -s "${dbfile}" ]
do
    dbfile=$(find ${TMPDIR}/pgcopydb/schema -name "source.db" -type f 2>/dev/null | head -1)
    sleep 1
done

sql="select pid from process where ps_type = 'prefetch'"
pidf=/tmp/prefetch.pid

while [ ! -s ${pidf} ]
do
    # sometimes we have "Error: database is locked", ignore
    sqlite3 -batch -bail -noheader "${dbfile}" "${sql}" > ${pidf} 2>/dev/null || echo error
    sleep 1
done

# allow replaying changes now that pgcopydb follow command is running
echo "Setting sentinel apply mode..."
pgcopydb stream sentinel set apply || { echo "Failed to set apply mode"; exit 1; }

# allow the catchup phase to finish, ensure the following data is streamed
sleep 2

# Insert test data: rows that exercise null handling, large batches, and updates
# across WAL segment boundaries to show the SQLite CDC pipeline is independent
# of PostgreSQL WAL segments (old file-based design depended on them critically).

# insert additional rows to exercise literal 'null' handling during live follow
psql -d ${PGCOPYDB_SOURCE_PGURI} <<'EOF'
insert into null_texts(text_col, json_col)
values ('null', '"null"'), (null, null);
EOF

psql -d ${PGCOPYDB_SOURCE_PGURI} -c 'select pg_switch_wal()'

# then insert another batch of 10 rows (21..30)
psql -v a=21 -v b=30 -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/dml.sql

psql -d ${PGCOPYDB_SOURCE_PGURI} -c 'select pg_switch_wal()'

# also insert data that won't fit in a single Unix PIPE buffer
psql -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/dml-bufsize.sql

psql -d ${PGCOPYDB_SOURCE_PGURI} -c 'select pg_switch_wal()'

# add some data to update_test table
psql -d ${PGCOPYDB_SOURCE_PGURI} << EOF
begin;
insert into update_test(id, name) values
(1, 'a'),
(2, 'b');
commit;

begin;
update update_test set name = 'c' where id = 1;
commit;

begin;
update update_test set name = 'd' where id = 2;
commit;

EOF

# grab the current LSN, it's going to be our streaming end position
echo "Setting endpos to current WAL position..."
pgcopydb stream sentinel set endpos --current || { echo "Failed to set endpos"; exit 1; }
echo "Final sentinel state:"
pgcopydb stream sentinel get

#
# Becaure we're using docker compose --abort-on-container-exit make sure
# that the other process in the pgcopydb service is done before exiting
# here.
#
sql="select exists(select 1 from pg_replication_slots where slot_name = 'pgcopydb')"

while [ `psql -At -d ${PGCOPYDB_SOURCE_PGURI} -c "${sql}"` = 't' ]
do
    sleep 1
done

#
# Give some time to the pgcopydb service to finish cleanup.
#
sleep 5
