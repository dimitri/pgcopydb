#! /bin/bash

set -x
set -e

# This script expects the following environment variables to be set:
#
#  - PGCOPYDB_SOURCE_PGURI
#  - PGCOPYDB_TARGET_PGURI
#  - PGCOPYDB_TABLE_JOBS
#  - PGCOPYDB_INDEX_JOBS

# Regression test for issue where REPLICA IDENTITY USING INDEX (on a non-PK
# unique index) caused the test_decoding parser to fail UPDATE messages with
# "WHERE clause columns not found".
#
# See src/bin/pgcopydb/ld_test_decoding.c:prepareUpdateTuppleArrays.

# make sure source and target databases are ready
pgcopydb ping

# apply schema + initial data on the source (target will be restored by clone)
psql -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/ddl.sql

# create replication slot + snapshot, then clone the initial data
coproc ( pgcopydb snapshot --follow --plugin test_decoding )

sleep 1

pgcopydb stream setup
pgcopydb clone

kill -TERM ${COPROC_PID}
wait ${COPROC_PID}

# Regression check: REPLICA IDENTITY USING INDEX must survive the clone,
# whether it points at a plain unique index (event_matches) or at the index
# backing a PRIMARY KEY constraint (event_matches_pk). pgcopydb builds indexes
# itself instead of going through pg_restore for the post-data section, and
# used to silently drop the ALTER TABLE ... REPLICA IDENTITY USING INDEX
# clause that pg_dump attaches to the INDEX/CONSTRAINT archive entry.
for table in event_matches event_matches_pk; do
	sql="select relreplident from pg_class where oid = '${table}'::regclass"

	src_ri=`psql -At -d ${PGCOPYDB_SOURCE_PGURI} -c "${sql}"`
	tgt_ri=`psql -At -d ${PGCOPYDB_TARGET_PGURI} -c "${sql}"`

	if [ "${src_ri}" != "${tgt_ri}" ]; then
		echo "REPLICA IDENTITY mismatch for ${table}: source=${src_ri} target=${tgt_ri}"
		exit 1
	fi

	if [ "${src_ri}" != "i" ]; then
		echo "Expected source table ${table} to have REPLICA IDENTITY USING INDEX (i), got ${src_ri}"
		exit 1
	fi
done

# produce CDC traffic on the source: INSERT, UPDATE, DELETE
psql -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/dml.sql

# mark the streaming end position at the current source WAL position
lsn=`psql -At -d ${PGCOPYDB_SOURCE_PGURI} -c 'select pg_current_wal_flush_lsn()'`

# prefetch captures changes from the replication slot and decodes them.
# Without the fix, prefetch/transform fails on UPDATEs against tables that
# use REPLICA IDENTITY USING INDEX on a non-PK index.
pgcopydb stream prefetch --resume --endpos "${lsn}" --notice

# allow replaying/catching-up changes and apply them to the target
pgcopydb stream sentinel set apply
pgcopydb stream catchup --resume --endpos "${lsn}" --notice

# cleanup replication slot + origin
pgcopydb stream cleanup

# verify source and target match for the table we care about.
#
# Key assertion: if the parser had failed UPDATE messages, the target would
# still hold the initial-* rows with their original names. We expect the
# target to reflect every INSERT, UPDATE, and DELETE from dml.sql.

sql="select id, name from event_matches order by id"

psql -At -F '|' -d ${PGCOPYDB_SOURCE_PGURI} -c "${sql}" > /tmp/s.out
psql -At -F '|' -d ${PGCOPYDB_TARGET_PGURI} -c "${sql}" > /tmp/t.out

diff /tmp/s.out /tmp/t.out
