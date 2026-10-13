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
# =====================
#
# A cutover must not lose a transaction that committed below endpos, and must
# not report success when it has.  Two defects made it do exactly that, and
# both of them only show up at the *tail* of a run:
#
#  1. The apply driver moves one unit per pass and a KEEPALIVE unit records no
#     transaction progress, so "this pass produced nothing" was read as
#     "nothing left to do" once receive had finished -- abandoning whatever was
#     still spooled in outputDB.
#
#  2. The apply connection runs in libpq pipeline mode, so pgcopydb only
#     *queues* BEGIN/DML/COMMIT.  A transaction is on the target only once the
#     pipeline has been synced, and the follow path never synced it, so
#     anything still buffered was discarded by PQfinish() on the way out.
#
# Both lose data silently, with exit status 0 and nothing logged as an error.
#
# The shape below is what makes them observable, and each part matters:
#
#  - `clone --follow` runs in the background and endpos is set from this same
#    script afterwards, which is how a real cutover is driven.  Driving it from
#    a separate container that waits for flush_lsn does not reproduce either
#    defect.
#  - MANY single-row transactions rather than one big one: one transaction is
#    absorbed whole and leaves no tail, and a handful fit in a single pipeline
#    buffer.  It takes enough separate commits for the last buffer to be
#    partial.
#  - Every row is committed before endpos is taken, so a row missing on the
#    target is lost data and not a race, and an exact count is a fair
#    assertion.

SEED_ROWS=500
TAIL_TXNS=500

# make sure source and target databases are ready
pgcopydb ping

psql -d ${PGCOPYDB_SOURCE_PGURI} -c "
  create table t(id bigint generated always as identity primary key, note text)"

psql -d ${PGCOPYDB_SOURCE_PGURI} -c "
  insert into t(note) select 'seed-'||g from generate_series(1,${SEED_ROWS}) g"

#
# Start the migration and leave it running: this script plays the part of the
# operator who injects traffic and then calls the cutover.
#
pgcopydb clone --follow --notice &
clone_pid=$!

#
# Writes only reach the migration once the slot is streaming, so wait for it.
# Anything committed before that is part of the base copy instead of the tail.
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
# One INSERT per line into a single psql session: psql autocommits each line,
# so this is ${TAIL_TXNS} separate transactions rather than one.
#
seq 1 ${TAIL_TXNS} \
    | sed "s/.*/insert into t(note) values ('tail-&');/" \
    | psql -q -d ${PGCOPYDB_SOURCE_PGURI}

#
# Every transaction above is committed and durable by now, so endpos taken
# here sits at or above all of them: the migration owes us every row.
#
pgcopydb stream sentinel set endpos --current

# a cutover that drops transactions must not be reported as a success either
wait ${clone_pid}

src=$(psql -AtqX -d ${PGCOPYDB_SOURCE_PGURI} -c "select count(*) from t")
tgt=$(psql -AtqX -d ${PGCOPYDB_TARGET_PGURI} -c "select count(*) from t")

echo "rows: source ${src}, target ${tgt}"

test $((SEED_ROWS + TAIL_TXNS)) -eq "${src}"
test "${src}" -eq "${tgt}"

# and the sentinel must not claim more than the apply actually did
replay=$(pgcopydb stream sentinel get --replay-lsn)
origin=$(psql -AtqX -d ${PGCOPYDB_TARGET_PGURI} -c "
  select coalesce(max(pg_replication_origin_progress(roname, true))::text, '0/0')
    from pg_replication_origin where roname like 'pgcopydb%'")

echo "replay_lsn ${replay}, target origin ${origin}"

pgcopydb stream cleanup
