#! /bin/bash

set -x
set -e

# Disable pager for psql to avoid hanging in non-interactive environments
export PAGER=cat

# This script expects the following environment variables to be set:
#
#  - PGCOPYDB_SOURCE_PGURI
#  - PGCOPYDB_TARGET_PGURI
#  - PGCOPYDB_TABLE_JOBS
#  - PGCOPYDB_INDEX_JOBS
#  - PGCOPYDB_OUTPUT_PLUGIN
#
# Regression test for sequence reset after a standalone `pgcopydb follow`
# reaches endpos -- the path used by resume-cdc helpers.
#
# Postgres logical decoding does not replicate sequences, and CDC replays
# INSERTs with OVERRIDING SYSTEM VALUE (explicit ids that do NOT advance the
# target sequence). So after the base copy the target sequence is stuck at the
# snapshot value, and only catches up to the source if follow_reset_sequences
# runs when follow reaches endpos. Without that reset the target sequence would
# hand out already-used ids after cutover.

# make sure source and target databases are ready
pgcopydb ping

psql -o /tmp/s.out -d ${PGCOPYDB_SOURCE_PGURI} -1 -f /usr/src/pagila/pagila-schema.sql
psql -o /tmp/d.out -d ${PGCOPYDB_SOURCE_PGURI} -1 -f /usr/src/pagila/pagila-data.sql

slot=pgcopydb
seq=rental_rental_id_seq

# create the replication slot that captures all the changes
# PGCOPYDB_OUTPUT_PLUGIN is set to wal2json in compose.yaml
coproc ( pgcopydb snapshot --follow --slot-name ${slot} )

sleep 1

# now setup the replication origin (target) and the pgcopydb.sentinel (source)
pgcopydb stream setup

# initial base copy: clones table data AND sequences at the snapshot value
pgcopydb clone

# sequence value on the target right after the base copy (the snapshot value)
tgt_after_clone=`psql -AtqX -d ${PGCOPYDB_TARGET_PGURI} -c "select last_value from ${seq}"`
echo "target ${seq} after clone: ${tgt_after_clone}"

# advance the source sequence well beyond the snapshot value
psql -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/dml.sql

# allow replaying changes, and set the end position to the current WAL location
pgcopydb stream sentinel set apply
pgcopydb stream sentinel set endpos --current

# standalone follow: replays the inserts and, on reaching endpos, resets the
# sequences on the target. This is the path used by resume-cdc helpers.
pgcopydb follow --resume

src_seq=`psql -AtqX -d ${PGCOPYDB_SOURCE_PGURI} -c "select last_value from ${seq}"`
tgt_seq=`psql -AtqX -d ${PGCOPYDB_TARGET_PGURI} -c "select last_value from ${seq}"`

echo "source ${seq}: ${src_seq}"
echo "target ${seq}: ${tgt_seq}"

# the sequence must have advanced during CDC, otherwise the test proves nothing
if [ "${src_seq}" -le "${tgt_after_clone}" ]
then
    echo "ERROR: source sequence did not advance during CDC"
    echo "  after clone: ${tgt_after_clone}, source now: ${src_seq}"
    echo "  the test is not exercising the sequence reset"
    exit 1
fi

# the target sequence must have been reset to match the source at endpos
if [ "${src_seq}" != "${tgt_seq}" ]
then
    echo "ERROR: target sequence ${seq} was not reset to match the source"
    echo "  source: ${src_seq}, target: ${tgt_seq}"
    echo "  (target stuck at snapshot value ${tgt_after_clone} means the reset"
    echo "   did not run after follow reached endpos)"
    exit 1
fi

echo ""
echo "follow-sequence-reset test: PASSED"
echo "  target ${seq} advanced from ${tgt_after_clone} to ${tgt_seq}, matching source"

kill -TERM ${COPROC_PID}
wait ${COPROC_PID}

# cleanup
pgcopydb stream cleanup
