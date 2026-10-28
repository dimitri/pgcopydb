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
# pgcopydb follow restarts its sub-processes when one of them fails, instead
# of exiting: the connection to the target is cut (what a failover of the
# target looks like), then the connection to the source is cut (the receive
# process), and the migration must carry on and end with every row on the
# target. Along the way:
#
#  - the restart is logged, so that an operator can see that it happened;
#  - on Postgres 17 and later, --enable-failover creates the replication slot
#    with the failover option, which a source of an older version refuses.

ROWS=200

# make sure source and target databases are ready
pgcopydb ping

psql -d ${PGCOPYDB_SOURCE_PGURI} -c "
  create table t(id bigint generated always as identity primary key, note text)"

psql -d ${PGCOPYDB_SOURCE_PGURI} -c "
  insert into t(note) select 'seed-'||g from generate_series(1,${ROWS}) g"

version=$(psql -AtqX -d ${PGCOPYDB_SOURCE_PGURI} -c "show server_version_num")

failover_opt=""
test "${version}" -ge 170000 && failover_opt="--enable-failover"

pgcopydb clone --follow --notice --max-restarts 3 ${failover_opt} \
    > /tmp/clone.log 2>&1 &
clone_pid=$!

# wait for the slot to be streaming: what comes next is the tail of the run
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

if [ -n "${failover_opt}" ]
then
    failover=$(psql -AtqX -d ${PGCOPYDB_SOURCE_PGURI} -c "
      select failover from pg_replication_slots where slot_name = 'pgcopydb'")
    echo "slot failover flag: ${failover}"
    test "${failover}" = "t"
fi

insert_rows() {
    seq 1 ${ROWS} \
        | sed "s/.*/insert into t(note) values ('$1-&');/" \
        | psql -q -d ${PGCOPYDB_SOURCE_PGURI}
}

# wait until the target has all the rows of the source
wait_for_rows() {
    for i in $(seq 1 120)
    do
        src=$(psql -AtqX -d ${PGCOPYDB_SOURCE_PGURI} -c "select count(*) from t")
        tgt=$(psql -AtqX -d ${PGCOPYDB_TARGET_PGURI} -c "select count(*) from t")
        echo "rows: source ${src}, target ${tgt}"
        test "${src}" = "${tgt}" && return 0
        sleep 1
    done

    return 1
}

insert_rows before
wait_for_rows

#
# 1. cut the connections to the target, the apply process fails
#
psql -d ${PGCOPYDB_TARGET_PGURI} -c "
  select pg_terminate_backend(pid)
    from pg_stat_activity
   where datname = current_database() and pid <> pg_backend_pid()"

insert_rows target-cut
wait_for_rows

#
# 2. cut the walsender connections of the source, the receive process
#    reconnects
#
psql -d ${PGCOPYDB_SOURCE_PGURI} -c "
  select pg_terminate_backend(pid)
    from pg_stat_activity
   where backend_type = 'walsender'"

insert_rows source-cut
wait_for_rows

pgcopydb stream sentinel set endpos --current

# a migration that needed restarts still ends with a success
wait ${clone_pid}

src=$(psql -AtqX -d ${PGCOPYDB_SOURCE_PGURI} -c "select count(*) from t")
tgt=$(psql -AtqX -d ${PGCOPYDB_TARGET_PGURI} -c "select count(*) from t")

echo "rows: source ${src}, target ${tgt}"

test $((ROWS * 4)) -eq "${src}"
test "${src}" -eq "${tgt}"

# the restart of the follow sub-processes has been logged
grep "Restarting follow sub-processes" /tmp/clone.log

pgcopydb stream cleanup
