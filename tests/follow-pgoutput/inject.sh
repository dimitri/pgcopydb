#! /bin/bash

set -x
set -e

# This script expects the following environment variables to be set:
#
#  - PGCOPYDB_SOURCE_PGURI
#  - PGCOPYDB_TARGET_PGURI
#  - PGCOPYDB_TABLE_JOBS
#  - PGCOPYDB_INDEX_JOBS

pgcopydb ping

#
# Follow coordinator TCP endpoint (provided by docker-compose). We remote-control
# the sentinel over TCP, so this container does NOT share the SQLite catalog
# volume with the follow process.
#
HP="--host ${PGCOPYDB_HOST} --port ${PGCOPYDB_PORT}"

#
# Only start injecting DML traffic once the follow process is streaming: that is
# exactly when `pgcopydb clone --follow` has finished the initial copy and opened
# its TCP coordinator.  Poll the coordinator over TCP until it answers.
#
until pgcopydb stream sentinel get ${HP} >/dev/null 2>&1
do
    sleep 1
done

#
# Inject a batch of DML changes.
# Then switch WAL segment to demonstrate that the SQLite CDC pipeline is
# independent of PostgreSQL WAL segment boundaries (in the old file-based
# design, WAL switches were critical milestones; now they're invisible).
#
psql -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/dml.sql
psql -d ${PGCOPYDB_SOURCE_PGURI} -c 'select pg_switch_wal()'

#
# Install the event trigger that turns DDL into a pg_logical_emit_message()
# "ddl" envelope. This is the only opt-in step of the feature: decoding and
# applying such a message is always on, with no CLI flag, on the follow side.
#
pgcopydb stream event-trigger --source ${PGCOPYDB_SOURCE_PGURI}

#
# Run a plain DDL statement: the event trigger fires automatically and
# emits the DDL as a message. No manual pg_logical_emit_message() call here;
# this proves the real, intended usage end to end.
#
psql -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/ddl-trigger.sql

# Inject another batch on the new WAL segment to show pipeline continues
# across segment boundaries without any special handling.
psql -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/dml.sql
psql -d ${PGCOPYDB_SOURCE_PGURI} -c 'select pg_switch_wal()'

#
# Inject pg_logical_emit_message() traffic that pgcopydb must safely ignore:
# a message of an unrecognized type (must be skipped, not break the stream)
# and a message with a different prefix (must be filtered out at decode
# time, never applied). Both exercise the always-on decoder without any
# opt-in from the user beyond having a pgoutput slot at all.
#
psql -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/messages.sql

#
# Nudge the WAL forward once more before capturing --current below.
#
# pg_current_wal_flush_lsn() (what --current uses) can land past the exact
# LSN that logical decoding's confirmed_flush_lsn will ever reach on its own
# when nothing else writes to the source afterward: receive's flush_lsn then
# sits a few bytes short of endpos forever, follow never declares itself
# done, and the apply pipeline (which only flushes at endpos/shutdown, see
# ld_apply.c:163-174) never applies anything it already queued -- with no
# error anywhere. A trivial WAL-generating statement here reliably closes
# that gap before endpos is captured.
psql -d ${PGCOPYDB_SOURCE_PGURI} -c 'select pg_switch_wal()'

# Set endpos to current flush LSN to signal follow where to stop (over TCP)
echo "Setting endpos to current WAL position..."
pgcopydb stream sentinel set endpos --current --debug ${HP} || { echo "Failed to set endpos"; exit 1; }
echo "Final sentinel state:"
pgcopydb stream sentinel get ${HP}

endpos=`pgcopydb stream sentinel get --endpos ${HP} 2>/dev/null`

if [ -z "${endpos}" ] || [ "${endpos}" = "0/0" ]
then
    echo "ERROR: endpos not set correctly (got: ${endpos})"
    exit 1
fi
echo "Successfully set endpos to ${endpos}"

#
# Because we're using docker-compose --abort-on-container-exit make sure
# that the other process in the pgcopydb service is done before exiting
# here. Wait for flush_lsn to reach endpos (indicates CDC pipeline caught up).
#
flushlsn="0/0"

while [ ${flushlsn} \< ${endpos} ]
do
    flushlsn=`pgcopydb stream sentinel get --flush-lsn ${HP} 2>/dev/null`
    sleep 1
done

#
# Give some time to the pgcopydb service to finish cleanup.
#
sleep 5
