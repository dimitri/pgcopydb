#! /bin/bash
#
# Repeatedly terminate pgcopydb's backends on the TARGET database, simulating
# the target dropping connections out from under a running clone.
#
# This script runs in the background while the main script drives the transfer
# with repeated `pgcopydb clone --resume`. It keeps killing pgcopydb target
# backends until it is told to stop (the main script creates the sentinel file
# named by $1).
#
# Unlike the reconnect-and-replay variant of this killer, nothing is spared:
# this test recovers purely through external resume, and resume restarts an
# unfinished table's COPY wholesale (TRUNCATE + re-COPY), so killing a COPY or
# TRUNCATE backend is just as recoverable as killing any other. Targeting every
# pgcopydb backend makes the test strictly harsher: any single clone invocation
# can be cut down at any phase, and only the outer resume loop puts it back.

set -u

stopfile="$1"

# pg_terminate_backend every connection whose application_name marks it as one
# of pgcopydb's (see pgsql.c: PGAPPNAME is always "pgcopydb[<pid>] ..."),
# except our own psql connection running this very query.
kill_sql="select count(pg_terminate_backend(pid))
            from pg_stat_activity
           where application_name like 'pgcopydb%'
             and pid <> pg_backend_pid();"

killed=0

while [ ! -f "${stopfile}" ]
do
    n=$(psql -AXqt -d "${PGCOPYDB_TARGET_PGURI}" -c "${kill_sql}" 2>/dev/null)

    if [ -n "${n}" ] && [ "${n}" -gt 0 ] 2>/dev/null
    then
        killed=$((killed + n))
        echo "killer: terminated ${n} pgcopydb target backend(s) (total ${killed})"
    fi

    # Short enough to cut down most clone invocations mid-flight, long enough to
    # let a resume make forward progress between kills.
    sleep 0.3
done

echo "killer: stopping, terminated ${killed} pgcopydb target backend(s) in total"

# Record the total for the main script to assert chaos actually happened.
echo "${killed}" > "${stopfile}.count"
