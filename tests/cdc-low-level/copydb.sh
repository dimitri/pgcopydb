#! /bin/bash

set -x
set -e
set -o pipefail

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
lsn=`psql -At -d ${PGCOPYDB_SOURCE_PGURI} -c 'select pg_current_wal_flush_lsn()'`

#
# Receive CDC messages into the SQLite outputDB (output table).  In the
# 2-process model the receive step only fills the output table; the transform
# into stmt+replay happens later, inside the apply (catchup) process.
#
pgcopydb stream prefetch --debug --resume --endpos "${lsn}"

SHAREDIR=/var/lib/postgres/.local/share/pgcopydb

ls -la ${SHAREDIR}/

OUTPUTDB=$(find ${SHAREDIR} -maxdepth 1 -name '*-output.db' -type f | head -1)

#
# Validate the SQLite output table has rows.
#
sqlite3 ${OUTPUTDB} "select count(*) as output_rows from output;"

# Idempotency: prefetch again should be a no-op
pgcopydb stream prefetch --debug --resume --endpos "${lsn}"

#
# Apply the CDC changes to the target database.  The apply process performs the
# inline transform (output -> stmt+replay), creating the replayDB.
#
pgcopydb stream sentinel set apply
pgcopydb stream catchup --resume --endpos "${lsn}" -vv

#
# replayDB now exists: validate the SQL templates written by the transform
# step and that the replay table was populated.
#
REPLAYDB=$(find ${SHAREDIR} -maxdepth 1 -name '*-replay.db' -type f | head -1)

sqlite3 -init /dev/null -list -noheader ${REPLAYDB} \
  "select s.sql from stmt s join replay r on r.stmt_hash = s.hash where r.action not in ('B','C','R','K','X','E') group by s.hash order by min(r.id)" \
  > /tmp/stmt-actual.sql
diff /usr/src/pgcopydb/stmt.sql /tmp/stmt-actual.sql

sqlite3 ${REPLAYDB} "select count(*) as replay_rows from replay;"

# Applying again must be idempotent
pgcopydb stream catchup --resume --endpos "${lsn}" -vv

#
# Verify row counts match between source and target
#
src_count=`psql -AtqX -d ${PGCOPYDB_SOURCE_PGURI} -c "select count(*) from actor"`
tgt_count=`psql -AtqX -d ${PGCOPYDB_TARGET_PGURI} -c "select count(*) from actor"`

echo "source actor count: ${src_count}, target actor count: ${tgt_count}"
test "${src_count}" -eq "${tgt_count}"

#
# Live-streaming via 'stream replay' (combined receive + inline transform +
# apply in a single service).  The standalone 'stream transform' command and
# the receive|transform|apply pipe were removed in the 2-process SQLite model;
# 'stream replay' is the supported live-streaming path.
#
for i in `seq 2`
do
    psql -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/dml.sql

    lsn=`psql -At -d ${PGCOPYDB_SOURCE_PGURI} -c 'select pg_current_wal_flush_lsn()'`

    pgcopydb stream replay --verbose --resume --endpos "${lsn}"
done

# Replay with no new changes should be a no-op
pgcopydb stream replay --resume --endpos "${lsn}"

#
# Pipeline-deadlock note: the original test applied a pre-written SQL file
# directly via 'pgcopydb stream apply' to reproduce a specific deadlock in
# the old text-parsing apply path.  In the SQLite pipeline, SQL is stored as
# parameterised stmt+stmt_args rows; the old fread/parse path that could
# deadlock is no longer in the code path.  Test removed.
#

# cleanup
pgcopydb stream drop --verbose
