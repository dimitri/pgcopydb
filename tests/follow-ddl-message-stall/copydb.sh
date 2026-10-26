#! /bin/bash

set -x
set -e

# This script expects the following environment variables to be set:
#
#  - PGCOPYDB_SOURCE_PGURI
#  - PGCOPYDB_TARGET_PGURI
#
# What this test covers
# ======================
#
# A DDL statement replicated as a pg_logical_emit_message() "ddl" envelope
# (see "pgcopydb stream event-trigger") must not stall the apply connection,
# and neither must an ordinary transaction committed right after it.
#
# The apply connection runs in libpq pipeline mode for its whole life (see
# stream_apply_flush_pipeline, ld_apply.c): pgsql_execute() only *queues*
# BEGIN/DML/COMMIT, and libpq does not actually write queued pipeline data
# to the socket until either 64kB accumulates (OUTBUFFER_THRESHOLD, libpq's
# fe-exec.c) or an explicit sync happens. Before stream_apply_transaction
# synced after every COMMIT, that sync only ever happened at an
# endpos/shutdown boundary -- so a small transaction, and everything queued
# after it, sat in the apply process's own memory, never reaching the
# target, for as long as the run kept going with nothing to trip that 64kB
# threshold.
#
# This test keeps the workload deliberately tiny and otherwise quiet (one
# narrow table, one DDL statement, one DML statement) specifically so
# nothing else pushes the pipeline buffer over that threshold and masks the
# bug by accident -- a busier fixture (e.g. the pagila-based follow tests,
# which also exercise DDL messages) can go either way run to run, depending
# on how much unrelated traffic happens to share the same buffer.
#
# The shape below is what makes the stall observable, and each part matters:
#
#  - `clone --follow` runs in the background, with no --endpos, and this
#    script polls the target directly while it is still running: endpos is
#    only set *after* the check below. Checking after endpos converges (as
#    the pagila-based follow tests do, for their own, different reasons)
#    would pass either way, because reaching endpos triggers its own flush
#    and silently papers over a regression here.
#  - The DML statement is a transaction of its own (no explicit BEGIN/COMMIT
#    around it, so psql autocommits it separately), queued right after the
#    DDL message's own transaction, not inside it: the DDL message's own
#    column can land as a side effect of enough *other* queued bytes
#    crossing the 64kB threshold, while the small transaction right after
#    it stays stuck behind it -- so checking the column alone is not enough
#    either.

pgcopydb ping

psql -d ${PGCOPYDB_SOURCE_PGURI} -c "
  create table t(id bigint generated always as identity primary key, note text)"

#
# Start the migration and leave it running: this script plays the part of
# the operator who installs the event trigger and injects traffic once
# streaming has started, then calls the cutover at the very end.
#
pgcopydb clone --follow --notice &
clone_pid=$!

#
# Writes only reach the migration once the slot is streaming, so wait for
# it before doing anything else.
#
slot_active=0

for i in $(seq 1 120)
do
    slot_active=$(psql -AtqX -d ${PGCOPYDB_SOURCE_PGURI} -c "
      select count(*) from pg_replication_slots
       where slot_name = 'pgcopydb' and active")

    test "${slot_active}" = "1" && break
    sleep 1
done

test "${slot_active}" = "1"

#
# Install the event trigger that turns DDL into a pg_logical_emit_message()
# "ddl" envelope. This is the only opt-in step of the feature: decoding and
# applying such a message is always on, with no CLI flag, on the follow
# side.
#
pgcopydb stream event-trigger --source ${PGCOPYDB_SOURCE_PGURI}

#
# Run a plain DDL statement: the event trigger fires automatically and
# emits the DDL as a message. This is a message-only transaction: no row
# changes, nothing else queued alongside it.
#
psql -d ${PGCOPYDB_SOURCE_PGURI} -c "alter table t add column price numeric(10,2) default 0"

#
# A separate, ordinary transaction right after it.
#
psql -d ${PGCOPYDB_SOURCE_PGURI} -c "insert into t(note) values ('tail-1')"

#
# Poll the target directly, with endpos still unset: this is the actual
# regression check, see the header comment above for why both the column
# and the row matter, and why this has to happen before endpos is set.
#
echo "Waiting for the DDL message and the DML row after it to land on the target, with endpos still unset..."
landed=0

for i in $(seq 1 60)
do
    if ! kill -0 ${clone_pid} 2>/dev/null
    then
        echo "pgcopydb clone --follow exited early, see above"
        break
    fi

    price_col=$(psql -AtqX -d ${PGCOPYDB_TARGET_PGURI} -c "
      select count(*) from information_schema.columns
       where table_name = 't' and column_name = 'price'" 2>/dev/null || echo 0)

    tail_row=$(psql -AtqX -d ${PGCOPYDB_TARGET_PGURI} -c "
      select count(*) from t where note = 'tail-1'" 2>/dev/null || echo 0)

    if [ "${price_col}" = "1" ] && [ "${tail_row}" = "1" ]
    then
        landed=1
        echo "Landed after ${i}s: t.price present, tail-1 row present."
        break
    fi

    sleep 1
done

if [ "${landed}" != "1" ]
then
    echo "ERROR: the DDL message and/or the DML row right after it never" \
         "reached the target mid-stream (apply pipeline stall after a" \
         "DDL message)."
    echo "t.price column present (expected 1): ${price_col}"
    echo "tail-1 row present (expected 1): ${tail_row}"
    kill ${clone_pid} 2>/dev/null || true
    exit 1
fi

#
# Now drive the normal cutover: everything from here on is already
# confirmed on the target, this just lets the migration finish cleanly.
#
pgcopydb stream sentinel set endpos --current
wait ${clone_pid}

src=$(psql -AtqX -d ${PGCOPYDB_SOURCE_PGURI} -c "select count(*) from t")
tgt=$(psql -AtqX -d ${PGCOPYDB_TARGET_PGURI} -c "select count(*) from t")

echo "rows: source ${src}, target ${tgt}"
test "${src}" -eq "${tgt}"

pgcopydb stream cleanup
