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

# Duty-cycle the chaos instead of killing continuously. A continuous killer can
# only ever pass on a machine fast enough to finish a whole `clone --resume`
# inside one sleep gap; on a slow CI runner every attempt is cut down and the
# transfer never converges. Instead we alternate a KILL window (fire every 0.3s)
# with a QUIET window (no kills at all) that is long enough for one resume to run
# to completion. Early resumes still get shredded during KILL windows, so the
# chaos stays real; a later near-converged resume finishes during a QUIET window
# regardless of how slow the runner is.
kill_burst=3     # seconds spent actively killing
quiet_window=15  # seconds of calm, long enough for one resume to converge

while [ ! -f "${stopfile}" ]
do
    # KILL window: terminate backends every 0.3s for kill_burst seconds.
    burst_end=$((SECONDS + kill_burst))

    while [ "${SECONDS}" -lt "${burst_end}" ] && [ ! -f "${stopfile}" ]
    do
        n=$(psql -AXqt -d "${PGCOPYDB_TARGET_PGURI}" -c "${kill_sql}" 2>/dev/null)

        if [ -n "${n}" ] && [ "${n}" -gt 0 ] 2>/dev/null
        then
            killed=$((killed + n))
            echo "killer: terminated ${n} pgcopydb target backend(s) (total ${killed})"
        fi

        sleep 0.3
    done

    # QUIET window: stop killing so a resume has an uninterrupted chance to
    # finish the transfer. Poll the stopfile so we exit promptly when told to.
    quiet_end=$((SECONDS + quiet_window))

    while [ "${SECONDS}" -lt "${quiet_end}" ] && [ ! -f "${stopfile}" ]
    do
        sleep 0.3
    done
done

echo "killer: stopping, terminated ${killed} pgcopydb target backend(s) in total"

# Record the total for the main script to assert chaos actually happened.
echo "${killed}" > "${stopfile}.count"
