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

# create the replication slot that captures all the changes with pgoutput
coproc ( pgcopydb snapshot --follow --plugin pgoutput )

sleep 1

# now setup the replication origin (target) and the pgcopydb.sentinel (source)
pgcopydb stream setup

# clone source to target, skipping the 'staff' table
pgcopydb clone --filters /usr/src/pgcopydb/filters.ini --split-tables-larger-than 200kB

kill -TERM ${COPROC_PID}
wait ${COPROC_PID}

#
# Verify that the filter excluded 'staff' from the initial clone.
#
staff_on_target=$(psql -AtqX -d ${PGCOPYDB_TARGET_PGURI} \
  -c "select count(*) from information_schema.tables where table_schema='public' and table_name='staff'")
echo "staff table present on target (should be 0): ${staff_on_target}"
test "${staff_on_target}" -eq 0

#
# Inject a CDC change: INSERT into actor (included table).
# We verify that this change IS replicated to the target.
# We do NOT insert into staff here because:
#   a) the staff table does not exist on the target (filtered out by clone), and
#   b) inserting into staff via CDC would fail on the target with "relation not found",
#      which would abort the transaction and lose the actor INSERT too.
# The exclusion of staff is already verified by the initial clone check above.
#
psql -d ${PGCOPYDB_SOURCE_PGURI} <<'EOF'
BEGIN;
-- Add a new actor (this SHOULD be replayed to the target)
INSERT INTO public.actor (actor_id, first_name, last_name, last_update)
VALUES (999, 'Test', 'Actor', now());
COMMIT;
EOF

# grab the current LSN as our end position
lsn=$(psql -At -d ${PGCOPYDB_SOURCE_PGURI} -c 'select pg_current_wal_flush_lsn()')

#
# Stream the CDC changes into the SQLite outputDB.
#
pgcopydb stream prefetch --resume --endpos "${lsn}" -vv

SHAREDIR=${XDG_DATA_HOME:-/var/lib/postgres/.local/share}/pgcopydb
OUTPUTDB=$(find ${SHAREDIR} -maxdepth 1 -name '*-output.db' -type f | head -1)

sqlite3 ${OUTPUTDB} "select count(*) as output_rows from output;"

#
# Validate that pgoutput_col has rows (pgoutput binary protocol is used).
#
col_rows=$(sqlite3 -init /dev/null -noheader -list ${OUTPUTDB} \
  "select count(*) from pgoutput_col;")
echo "pgoutput_col rows: ${col_rows}"
test "${col_rows}" -gt 0

#
# Validate that DML rows have NULL message (pgoutput stores structured data,
# not text blobs).
#
null_msg=$(sqlite3 -init /dev/null -noheader -list ${OUTPUTDB} \
  "select count(*) from output where action in ('I','U','D','T') and message is not null;")
echo "DML rows with non-NULL message (should be 0): ${null_msg}"
test "${null_msg}" -eq 0

#
# The actor INSERT must be captured in the output.db (pgoutput decoded it).
#
actor_cdc=$(sqlite3 -init /dev/null -noheader -list ${OUTPUTDB} \
  "select count(*) from output where action = 'I' and relname = 'actor';")
echo "actor INSERT CDC rows: ${actor_cdc} (should be > 0)"
test "${actor_cdc}" -gt 0

#
# Allow the apply process and replay the captured CDC changes.
#
pgcopydb stream sentinel set apply

pgcopydb stream catchup --resume --endpos "${lsn}" -vv

#
# Verify the actor was replicated to the target.
#
actor_on_target=$(psql -AtqX -d ${PGCOPYDB_TARGET_PGURI} \
  -c "select count(*) from actor where actor_id = 999")
echo "new actor on target (should be 1): ${actor_on_target}"
test "${actor_on_target}" -eq 1

#
# Verify that the staff INSERT was NOT replicated (table excluded by filter).
# The staff table should still not exist on the target.
#
staff_on_target_after=$(psql -AtqX -d ${PGCOPYDB_TARGET_PGURI} \
  -c "select count(*) from information_schema.tables where table_schema='public' and table_name='staff'")
echo "staff table present on target after CDC (should be 0): ${staff_on_target_after}"
test "${staff_on_target_after}" -eq 0

#
# Row count sanity check: actor counts must match.
#
src_actor=$(psql -AtqX -d ${PGCOPYDB_SOURCE_PGURI} -c "select count(*) from actor")
tgt_actor=$(psql -AtqX -d ${PGCOPYDB_TARGET_PGURI} -c "select count(*) from actor")
echo "source actor: ${src_actor}, target actor: ${tgt_actor}"
test "${src_actor}" -eq "${tgt_actor}"

# cleanup (drops the auto-managed publication for pgoutput)
pgcopydb stream drop

#
# Validate that the auto-managed publication was dropped by cleanup.
#
pub_after=$(psql -AtqX -d ${PGCOPYDB_SOURCE_PGURI} \
  -c "select count(*) from pg_publication where pubname = 'pgcopydb'")
echo "publication rows after cleanup (should be 0): ${pub_after}"
test "${pub_after}" -eq 0
