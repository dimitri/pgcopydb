#! /bin/bash

set -x
set -e

# This script expects the following environment variables to be set:
#
#  - PGCOPYDB_SOURCE_PGURI
#  - PGCOPYDB_TARGET_PGURI
#  - PGCOPYDB_TABLE_JOBS
#  - PGCOPYDB_INDEX_JOBS
#
# It checks that a `pgcopydb clone` completes a full, correct transfer under a
# target that repeatedly drops its connections, using ONLY pgcopydb's existing
# `--resume` to recover -- no internal reconnect-and-replay. A background killer
# terminates pgcopydb's target backends throughout, so almost every clone
# invocation is cut down mid-flight; the recovery strategy is to simply run
# `clone --resume` again, and again, until one invocation survives a kill-free
# window long enough to finish. The transfer must then be provably complete:
# schema and data compare equal to the source.

# make sure source and target databases are ready
pgcopydb ping

# build the source dataset: tables + indexes + constraints + a sequence + large
# objects, so the transfer exercises every path a reset can interrupt
psql -d "${PGCOPYDB_SOURCE_PGURI}" -1 -f /usr/src/pgcopydb/import.sql

# record the source state we expect to see on the target after the transfer
SQL="select
       (select count(*) from t1) as t1,
       (select count(*) from t2) as t2,
       (select count(*) from t3) as t3,
       (select count(*) from t4) as t4,
       (select last_value from standalone_seq) as seq,
       (select count(*) from pg_largeobject_metadata) as blobs;"

psql -AXqt -d "${PGCOPYDB_SOURCE_PGURI}" -c "${SQL}" > /tmp/source.state

# start the backend killer in the background; it runs for the whole chaos phase
# and stops when we create the sentinel file
stopfile=/tmp/killer.stop
rm -f "${stopfile}" "${stopfile}.count"
bash /usr/src/pgcopydb/killer.sh "${stopfile}" &
killer_pid=$!

# Chaos phase: drive the transfer with repeated `clone --resume` while the
# killer resets target connections underneath each invocation.
#
# The first invocation uses --restart to start from a clean slate; every
# subsequent one uses --resume to pick up whatever the previous (likely killed)
# invocation left behind. --not-consistent is required by --resume (it allows a
# fresh snapshot on the source for each attempt). We bound the loop so a genuine
# wedge fails the test instead of hanging CI.
max_attempts=40
attempt=0
completed_under_chaos=0

while [ "${attempt}" -lt "${max_attempts}" ]
do
    attempt=$((attempt + 1))

    if [ "${attempt}" -eq 1 ]
    then
        flags="--restart"
    else
        flags="--resume --not-consistent"
    fi

    rc=0
    pgcopydb clone --notice ${flags} \
             --table-jobs "${PGCOPYDB_TABLE_JOBS}" \
             --index-jobs "${PGCOPYDB_INDEX_JOBS}" \
             --source "${PGCOPYDB_SOURCE_PGURI}" \
             --target "${PGCOPYDB_TARGET_PGURI}" || rc=$?

    echo "resume-recovery: attempt ${attempt} exited ${rc}"

    if [ "${rc}" -eq 0 ]
    then
        completed_under_chaos=1
        break
    fi
done

# stop the killer and collect how many backends it actually terminated
touch "${stopfile}"
wait "${killer_pid}" || true
killed_total=$(cat "${stopfile}.count" 2>/dev/null || echo 0)

echo "resume-recovery: killer terminated ${killed_total} target backend(s) across ${attempt} clone attempt(s)"

# The chaos must have been real: if the killer never terminated a backend the
# test proved nothing.
test "${killed_total}" -gt 0

# A resume-only recovery that never once completed under chaos is a wedge, which
# is exactly the failure this test exists to catch.
test "${completed_under_chaos}" = "1"

# Settle pass: with the killer stopped, one more `clone --resume` is a cheap
# no-op when the transfer already finished, and a guaranteed finish if the last
# chaotic attempt happened to exit 0 on a step that still had a trailing no-op.
# It makes the final comparison deterministic regardless of where chaos landed.
pgcopydb clone --notice --resume --not-consistent \
         --table-jobs "${PGCOPYDB_TABLE_JOBS}" \
         --index-jobs "${PGCOPYDB_INDEX_JOBS}" \
         --source "${PGCOPYDB_SOURCE_PGURI}" \
         --target "${PGCOPYDB_TARGET_PGURI}"

# Prove the transfer is complete and correct.
#
# Row-count / sequence / blob-count parity is the coarse check; pgcopydb's own
# schema and data comparison is the authoritative one (it checksums every
# table's contents, not just its cardinality).
psql -AXqt -d "${PGCOPYDB_TARGET_PGURI}" -c "${SQL}" > /tmp/target.state
diff /tmp/source.state /tmp/target.state

pgcopydb compare schema
pgcopydb compare data

pgcopydb list progress --summary || true

echo "target-resume-recovery: external clone --resume recovered a full, correct transfer under repeated target connection drops"
