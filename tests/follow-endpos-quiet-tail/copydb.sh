#! /bin/bash

set -x
set -e

# This script expects the following environment variables to be set:
#
#  - PGCOPYDB_SOURCE_PGURI
#  - PGCOPYDB_TARGET_PGURI
#  - PGCOPYDB_TABLE_JOBS
#
# What this test covers
# ======================
#
# `pgcopydb stream sentinel set endpos --current` used to read
# pg_current_wal_flush_lsn() directly. That LSN is not guaranteed to be one
# logical decoding will ever report as confirmed-flushed on its own: a
# replication slot only confirms flush up to record boundaries it has
# decoded, and an arbitrary WAL offset need not fall on one. When nothing
# writes to the source again after --current is captured, receive's own
# flush_lsn can sit a few bytes short of endpos forever. receive then never
# signals "done", and apply -- which only flushes its own libpq pipeline once
# endpos is reached -- never applies the work it already queued. No error is
# logged anywhere; `follow` simply never finishes.
#
# This is exactly the shape of a real cutover done by hand: stop writing,
# then ask `follow` to catch up and stop. follow-endpos-tail (a sibling test)
# exercises many commits landing right up against --current and passes
# today; what it does not isolate is total silence on the source *after*
# --current is captured, which is the part that actually triggers this.
#
# The timeout below turns a hang into a clear, fast test failure instead of
# eating the whole CI job's time budget.

WAIT_TIMEOUT=60

pgcopydb ping

psql -d ${PGCOPYDB_SOURCE_PGURI} -c "
  create table t(id bigint generated always as identity primary key, note text)"

psql -d ${PGCOPYDB_SOURCE_PGURI} -c "
  insert into t(note) select 'seed-'||g from generate_series(1,100) g"

pgcopydb clone --follow --notice &
clone_pid=$!

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

# One last write, then go quiet: nothing else touches the source again
# before --current is captured below.
psql -d ${PGCOPYDB_SOURCE_PGURI} -c "insert into t(note) values ('tail-last-row')"

# --current must still find an endpos that is actually reachable even
# though nothing will write to the source again.
pgcopydb stream sentinel set endpos --current

# Without the fix, this hangs forever: turn that into a clear failure.
waited=0
while kill -0 ${clone_pid} 2>/dev/null
do
    if [ "${waited}" -ge "${WAIT_TIMEOUT}" ]
    then
        echo "FAIL: follow did not reach endpos within ${WAIT_TIMEOUT}s " \
             "(flush_lsn stuck short of endpos with a quiet source: see " \
             "pgsql_emit_endpos_marker in pgsql.c)"
        kill -9 ${clone_pid} 2>/dev/null || true
        exit 1
    fi

    sleep 1
    waited=$((waited + 1))
done

wait ${clone_pid}

src=$(psql -AtqX -d ${PGCOPYDB_SOURCE_PGURI} -c "select count(*) from t")
tgt=$(psql -AtqX -d ${PGCOPYDB_TARGET_PGURI} -c "select count(*) from t")

echo "rows: source ${src}, target ${tgt}"

test "${src}" -eq 101
test "${src}" -eq "${tgt}"

pgcopydb stream cleanup
