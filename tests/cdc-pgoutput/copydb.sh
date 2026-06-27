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

psql -o /tmp/s.out -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/special-ddl.sql
psql -o /tmp/s.out -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/special-dml.sql

# create the replication slot that captures all the changes (with pgoutput plugin)
coproc ( pgcopydb snapshot --follow --plugin pgoutput )

sleep 1

# now setup the replication origin (target) and the pgcopydb.sentinel (source)
pgcopydb stream setup

# pgcopydb clone uses the environment variables
pgcopydb clone --split-tables-larger-than 200kB

kill -TERM ${COPROC_PID}
wait ${COPROC_PID}

# now that the copying is done, inject some SQL DML changes to the source
psql -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/dml.sql
psql -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/special-dml.sql

# grab the current LSN, it's going to be our streaming end position
lsn=`psql -At -d ${PGCOPYDB_SOURCE_PGURI} -c 'select pg_current_wal_flush_lsn()'`

#
# Receive CDC messages into the SQLite outputDB (output table + pgoutput_col table).
#
pgcopydb stream prefetch --resume --endpos "${lsn}" -vv

SHAREDIR=${XDG_DATA_HOME:-/var/lib/postgres/.local/share}/pgcopydb

ls -la ${SHAREDIR}/

OUTPUTDB=$(find ${SHAREDIR} -maxdepth 1 -name '*-output.db' -type f | head -1)

sqlite3 ${OUTPUTDB} "select count(*) as output_rows from output;"

#
# Validate that the pgoutput_col table exists and has rows.
#
col_rows=$(sqlite3 -init /dev/null -noheader -list ${OUTPUTDB} \
  "select count(*) from pgoutput_col;")
echo "pgoutput_col rows: ${col_rows}"
test "${col_rows}" -gt 0

#
# Validate that DML rows in output have nspname/relname set and NULL message.
#
null_msg=$(sqlite3 -init /dev/null -noheader -list ${OUTPUTDB} \
  "select count(*) from output where action in ('I','U','D','T') and message is not null;")
echo "DML rows with non-NULL message (should be 0): ${null_msg}"
test "${null_msg}" -eq 0

nspname_set=$(sqlite3 -init /dev/null -noheader -list ${OUTPUTDB} \
  "select count(*) from output where action in ('I','U','D') and nspname is not null;")
echo "DML rows with nspname set: ${nspname_set}"
test "${nspname_set}" -gt 0

#
# Validate REPLICA IDENTITY DEFAULT DELETE (K-section):
#   For the 'rental' DELETE, pgoutput sends a 'K' tuple with only the primary
#   key column (rental_id) as status='t' and all other columns as status='n'.
#   The 'n' columns must be stored in pgoutput_col but NOT used in the WHERE
#   clause (stmt.sql must show only "WHERE rental_id = $1").
#
k_t_cols=$(sqlite3 -init /dev/null -noheader -list ${OUTPUTDB} \
  "select count(*) from pgoutput_col c
   join output o on o.id = c.output_id
   where o.action = 'D' and o.relname = 'rental'
     and c.section = 'K' and c.status = 't';")
k_n_cols=$(sqlite3 -init /dev/null -noheader -list ${OUTPUTDB} \
  "select count(*) from pgoutput_col c
   join output o on o.id = c.output_id
   where o.action = 'D' and o.relname = 'rental'
     and c.section = 'K' and c.status = 'n';")
echo "rental DELETE K-section: status='t' cols=${k_t_cols} (should be 1), status='n' cols=${k_n_cols} (should be 6)"
test "${k_t_cols}" -eq 1
test "${k_n_cols}" -eq 6

#
# Validate REPLICA IDENTITY FULL DELETE (O-section) with genuine NULL:
#   For the 'address' DELETE, pgoutput sends an 'O' tuple where address2 is
#   genuinely NULL (status='n'). That column must appear in the WHERE clause
#   as "address2 IS NULL".
#
o_null_cols=$(sqlite3 -init /dev/null -noheader -list ${OUTPUTDB} \
  "select count(*) from pgoutput_col c
   join output o on o.id = c.output_id
   where o.action = 'D' and o.relname = 'address'
     and c.section = 'O' and c.name = 'address2' and c.status = 'n';")
echo "address DELETE O-section address2 NULL cols: ${o_null_cols} (should be > 0)"
test "${o_null_cols}" -gt 0

#
# Validate pgoutput_col structured output.
# If the golden file is empty (first run), capture the output as the golden file.
# On subsequent runs, diff against the golden file.
#
sqlite3 -init /dev/null -json ${OUTPUTDB} \
  "select o.action, o.nspname, o.relname, o.old_type,
          json_group_array(
            json_object('section',c.section,'pos',c.pos,'name',c.name,
                        'status',c.status,'value',c.value)
          ) as cols
   from output o
   left join pgoutput_col c on c.output_id = o.id
   where o.action not in ('K','X','E','B','C')
   group by o.id
   order by o.id" \
  > /tmp/result.jsonl

if [ -s /usr/src/pgcopydb/output.pgout ]; then
  diff /usr/src/pgcopydb/output.pgout /tmp/result.jsonl
else
  echo "NOTE: output.pgout is empty — first run, accepting output as golden file."
  echo "      Copy /tmp/result.jsonl into tests/cdc-pgoutput/output.pgout to enable comparison."
  cat /tmp/result.jsonl | head -5
fi

#
# Run prefetch again — should be a no-op (idempotent).
#
pgcopydb stream prefetch --resume --endpos "${lsn}" -vv

#
# Allow the apply process and apply the CDC changes to the target.
#
pgcopydb stream sentinel set apply

pgcopydb stream catchup --resume --endpos "${lsn}" -vv

#
# replayDB now exists: validate the replay table and the SQL templates.
#
REPLAYDB=$(find ${SHAREDIR} -maxdepth 1 -name '*-replay.db' -type f | head -1)

sqlite3 ${REPLAYDB} "select count(*) as replay_rows from replay;"

sqlite3 -init /dev/null -list -noheader ${REPLAYDB} \
  "select s.sql from stmt s join replay r on r.stmt_hash = s.hash where r.action not in ('B','C','R','K','X','E') group by s.hash order by min(r.id)" \
  > /tmp/stmt-actual.sql
diff /usr/src/pgcopydb/stmt.sql /tmp/stmt-actual.sql

# Applying the same endpos again should be a no-op (already reached).
pgcopydb stream catchup --resume --endpos "${lsn}" -vv

#
# Verify that the data made it to the target.
#
src_count=`psql -AtqX -d ${PGCOPYDB_SOURCE_PGURI} -c "select count(*) from actor"`
tgt_count=`psql -AtqX -d ${PGCOPYDB_TARGET_PGURI} -c "select count(*) from actor"`

echo "source actor count: ${src_count}, target actor count: ${tgt_count}"
test "${src_count}" -eq "${tgt_count}"

#
# Multi-row DELETE batching validation (issue #828).
#
multi_delete_count=$(sqlite3 -init /dev/null -list -noheader ${REPLAYDB} \
    "select count(*)
     from stmt s
     join replay r on r.stmt_hash = s.hash
     where s.sql like 'DELETE FROM \"public\".\"multi_delete_test\" %'")

echo "multi_delete_count (expected 1 batched statement): ${multi_delete_count}"
test "${multi_delete_count}" -eq 1

multi_delete_composite_count=$(sqlite3 -init /dev/null -list -noheader ${REPLAYDB} \
    "select count(*)
     from stmt s
     join replay r on r.stmt_hash = s.hash
     where s.sql like 'DELETE FROM \"public\".\"multi_delete_composite_test\" %'")

echo "multi_delete_composite_count (expected 1 batched statement): ${multi_delete_composite_count}"
test "${multi_delete_composite_count}" -eq 1

src_multi=`psql -AtqX -d ${PGCOPYDB_SOURCE_PGURI} -c "select count(*) from multi_delete_test"`
tgt_multi=`psql -AtqX -d ${PGCOPYDB_TARGET_PGURI} -c "select count(*) from multi_delete_test"`
echo "multi_delete_test source: ${src_multi}, target: ${tgt_multi}"
test "${src_multi}" -eq "${tgt_multi}"
test "${tgt_multi}" -eq 0

src_comp=`psql -AtqX -d ${PGCOPYDB_SOURCE_PGURI} -c "select count(*) from multi_delete_composite_test"`
tgt_comp=`psql -AtqX -d ${PGCOPYDB_TARGET_PGURI} -c "select count(*) from multi_delete_composite_test"`
echo "multi_delete_composite_test source: ${src_comp}, target: ${tgt_comp}"
test "${src_comp}" -eq "${tgt_comp}"
test "${tgt_comp}" -eq 0

#
# Verify that a second run of pgcopydb snapshot --follow does not reprint a
# stale snapshot identifier (issue #559).
#
# Use an isolated slot name and work directory so this phase does not
# interfere with the main test state.
#
# Phase 1: first run creates the slot and exports a snapshot, writing the
#   identifier to stdout.  After SIGTERM the snapshot file persists on disk
#   (it is needed for --resume commands) but the replication connection is
#   closed so the snapshot is no longer live on the server.
#
# Phase 2: second run finds the slot in the SQLite catalog.  It must NOT
#   reprint the old identifier — stdout must be empty.
#

SNAP559_SLOT=pgcopydb559
SNAP559_DIR=/tmp/pgcopydb559

pgcopydb snapshot --follow \
	--plugin pgoutput \
	--slot-name ${SNAP559_SLOT} \
	--dir ${SNAP559_DIR} \
	>/tmp/snapshot559a.out 2>/dev/null &
SNAP559_PID=$!

sleep 2

snapshot_a=$(cat /tmp/snapshot559a.out)
echo "first run snapshot: ${snapshot_a}"
test -n "${snapshot_a}"

test -f "${SNAP559_DIR}/snapshot"
echo "snapshot file exists while process is running"

kill -TERM ${SNAP559_PID}
wait ${SNAP559_PID} 2>/dev/null || true
echo "snapshot holder exited cleanly"

pgcopydb snapshot --follow \
	--plugin pgoutput \
	--slot-name ${SNAP559_SLOT} \
	--dir ${SNAP559_DIR} \
	>/tmp/snapshot559b.out 2>/dev/null &
SNAP559B_PID=$!

sleep 2

kill -TERM ${SNAP559B_PID}
wait ${SNAP559B_PID} 2>/dev/null || true

snapshot_b=$(cat /tmp/snapshot559b.out)
echo "second run snapshot: '${snapshot_b}'"
test -z "${snapshot_b}"
echo "second run correctly produced no stale snapshot identifier"

# Drop the isolated replication slot and its auto-managed publication.
psql -d "${PGCOPYDB_SOURCE_PGURI}" \
	-c "select pg_drop_replication_slot('${SNAP559_SLOT}')" 2>/dev/null || true
psql -d "${PGCOPYDB_SOURCE_PGURI}" \
	-c "DROP PUBLICATION IF EXISTS ${SNAP559_SLOT}" 2>/dev/null || true

# cleanup (drops the auto-managed publication for pgoutput)
pgcopydb stream cleanup

#
# Validate that the auto-managed publication was dropped by cleanup.
# The publication name defaults to the slot name ("pgcopydb").
#
pub_after=$(psql -AtqX -d ${PGCOPYDB_SOURCE_PGURI} \
  -c "select count(*) from pg_publication where pubname = 'pgcopydb'")
echo "publication rows after cleanup (should be 0): ${pub_after}"
test "${pub_after}" -eq 0
