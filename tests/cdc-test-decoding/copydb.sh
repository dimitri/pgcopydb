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

# alter the pagila schema to allow capturing DDLs without pkey
psql -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/ddl.sql

# create the replication slot that captures all the changes
# PGCOPYDB_OUTPUT_PLUGIN is set to test_decoding in docker-compose.yml
coproc ( pgcopydb snapshot --follow )

sleep 1

# now setup the replication origin (target) and the pgcopydb.sentinel (source)
pgcopydb stream setup

# pgcopydb clone uses the environment variables
pgcopydb clone

kill -TERM ${COPROC_PID}
wait ${COPROC_PID}

# now that the copying is done, inject some SQL DML changes to the source
psql -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/dml.sql

# grab the current LSN, it's going to be our streaming end position
lsn=`psql -At -d ${PGCOPYDB_SOURCE_PGURI} -c 'select pg_current_wal_lsn()'`

#
# Receive CDC messages into the SQLite replayDB and transform them.
#
pgcopydb stream prefetch --resume --endpos "${lsn}" --debug

SHAREDIR=/var/lib/postgres/.local/share/pgcopydb

ls -la ${SHAREDIR}/

DBFILE=$(ls ${SHAREDIR}/*.db | head -1)

#
# Validate SQL templates written by the transform step.
#
sqlite3 -init /dev/null -list -noheader ${DBFILE} \
  "select s.sql from stmt s join replay r on r.stmt_hash = s.hash where r.action not in ('B','C','R','K','X','E') group by s.hash order by min(r.id)" \
  > /tmp/stmt-actual.sql
diff /usr/src/pgcopydb/stmt.sql /tmp/stmt-actual.sql

#
# Validate that the SQLite output and replay tables were populated.
#
sqlite3 ${DBFILE} <<'EOF'
.echo on
select count(*) as output_rows from output;
select count(*) as replay_rows from replay;
EOF

# Idempotency: prefetch again should be a no-op
pgcopydb stream prefetch --resume --endpos "${lsn}" --notice

#
# Allow apply and catch up with the replayDB.
#
pgcopydb stream sentinel set apply
pgcopydb stream catchup --resume --endpos "${lsn}" -vv

# Applying again must be idempotent
pgcopydb stream catchup --resume --endpos "${lsn}" -vv

#
# Row-count validation
#
src_count=`psql -AtqX -d ${PGCOPYDB_SOURCE_PGURI} -c "select count(*) from actor"`
tgt_count=`psql -AtqX -d ${PGCOPYDB_TARGET_PGURI} -c "select count(*) from actor"`
echo "source actor count: ${src_count}, target actor count: ${tgt_count}"
test "${src_count}" -eq "${tgt_count}"

# cleanup
pgcopydb stream cleanup
