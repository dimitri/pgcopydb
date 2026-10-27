#! /bin/bash

set -x
set -e

# This script expects the following environment variables to be set:
#
#  - PGCOPYDB_SOURCE_PGURI
#  - PGCOPYDB_TARGET_PGURI
#
# Benchmark, not a pass/fail test: how long does `clone --follow` take to
# catch the target up on a stream of many small (single-row) transactions,
# the exact shape #704 (libpq pipeline mode) and the pipeline-sync batching
# on top of it were built for -- one round trip per transaction defeats
# pipelining just as surely as no pipelining at all.

N=${BENCH_TXNS:-2000}

pgcopydb ping

psql -d ${PGCOPYDB_SOURCE_PGURI} -c "
  create table bench(id bigint generated always as identity primary key, note text)"

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

cat > /tmp/insert.sql <<'EOF'
insert into bench(note) values ('row');
EOF

CLIENTS=${BENCH_CLIENTS:-50}
JOBS=${BENCH_JOBS:-8}
TXN_PER_CLIENT=$(( N / CLIENTS ))

echo "== running pgbench: ${N} single-row transactions, ${CLIENTS} concurrent clients =="
echo "== (a lone serial connection never builds a backlog; concurrency is what"
echo "==  stresses apply's round-trip-per-commit cost, which is what pipeline"
echo "==  batching targets) =="
pgbench_start_ns=$(date +%s%N)

pgbench -n -c "${CLIENTS}" -j "${JOBS}" -t "${TXN_PER_CLIENT}" -f /tmp/insert.sql ${PGCOPYDB_SOURCE_PGURI}
N=$(( CLIENTS * TXN_PER_CLIENT ))

pgbench_end_ns=$(date +%s%N)
insert_elapsed_ms=$(( (pgbench_end_ns - pgbench_start_ns) / 1000000 ))

echo "== source insert done in ${insert_elapsed_ms} ms, waiting for target to catch up =="

caught_up=0
caughtup_end_ns=0

for i in $(seq 1 1200)
do
    tgt=$(psql -AtqX -d ${PGCOPYDB_TARGET_PGURI} -c "select count(*) from bench" 2>/dev/null || echo 0)

    if [ "${tgt}" = "${N}" ]
    then
        caughtup_end_ns=$(date +%s%N)
        caught_up=1
        break
    fi

    sleep 0.1
done

if [ "${caught_up}" != "1" ]
then
    echo "ERROR: target never caught up to ${N} rows (last count: ${tgt})"
    kill ${clone_pid} 2>/dev/null || true
    exit 1
fi

catchup_elapsed_ms=$(( (caughtup_end_ns - pgbench_end_ns) / 1000000 ))
total_elapsed_ms=$(( (caughtup_end_ns - pgbench_start_ns) / 1000000 ))

apply_tps=0
if [ "${catchup_elapsed_ms}" -gt 0 ]
then
    apply_tps=$(( N * 1000 / catchup_elapsed_ms ))
fi

echo "=== BENCHMARK RESULT ==="
echo "transactions: ${N}"
echo "source insert elapsed: ${insert_elapsed_ms} ms"
echo "target catch-up elapsed (after source insert done): ${catchup_elapsed_ms} ms"
echo "total elapsed (source start to target caught up): ${total_elapsed_ms} ms"
echo "effective apply throughput: ${apply_tps} tx/s"
echo "========================"

pgcopydb stream sentinel set endpos --current
wait ${clone_pid}

pgcopydb stream cleanup
