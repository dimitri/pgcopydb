#!/usr/bin/env bash
#
# Runs the pgcopydb test suite locally in parallel, mirroring the
# GitHub Actions tiered flow (run-tests-tiered.yml).
#
# Usage:
#   PGVERSION=18 DOCKER=podman bash tests/run-parallel.sh
#   JOBS=6 PGVERSION=18 DOCKER=podman bash tests/run-parallel.sh
#
# Environment:
#   PGVERSION  - Postgres version to test against (default: 18)
#   DOCKER     - Container runtime: docker or podman (default: auto-detect)
#   JOBS       - Max parallel test jobs (default: 4)
#   TIMEOUT    - Seconds per test before kill (default: 300, matching CI)

set -euo pipefail

PGVERSION=${PGVERSION:-18}
DOCKER=${DOCKER:-$(command -v podman 2>/dev/null || command -v docker 2>/dev/null)}
JOBS=${JOBS:-4}
TIMEOUT=${TIMEOUT:-300}

export PGVERSION DOCKER

# Change to repo root regardless of where script is invoked from
cd "$(dirname "$0")/.."

LOGDIR=$(mktemp -d /tmp/pgcopydb-tests-XXXXXX)

log() { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }

# macOS ships without timeout; use gtimeout (brew install coreutils) if available
TIMEOUT_CMD=$(command -v gtimeout 2>/dev/null || command -v timeout 2>/dev/null || echo "")
if [ -z "${TIMEOUT_CMD}" ]; then
    log "WARNING: no timeout command found — tests will not be killed on hang"
    run_with_timeout() { "$@"; }
else
    run_with_timeout() { "${TIMEOUT_CMD}" "${TIMEOUT}" "$@"; }
fi
export -f run_with_timeout
export TIMEOUT_CMD

# ── Tier 2: Smoke tests (run first, fast) ────────────────────────────────────
TIER2=(unit)
# tests/ci is just banned.h.sh - not container-based, skip for now

# ── Tier 3: Integration tests (same matrix as GH Actions) ───────────────────
TIER3=(
    pagila
    pagila-multi-steps
    pagila-standby
    blobs
    filtering
    filtering-standby
    extensions
    timescaledb
    skip-publications
    skip-vacuum
    skip-large-objects
    cdc-low-level
    cdc-test-decoding
    cdc-endpos-between-transaction
    cdc-filtering
    cdc-wal2json
    follow-wal2json
    follow-standby
    follow-9.6
    follow-data-only
    follow-target-reconnect
    endpos-in-multi-wal-txn
    blob-snapshot-release
    follow-defer-indexes
)

run_test() {
    local test=$1
    local logfile="${LOGDIR}/${test}.log"
    local result="${LOGDIR}/${test}.result"

    # Pre-create volume for tests that declare it external: true
    ${DOCKER} volume create "${test}" >/dev/null 2>&1 || true

    log "START: ${test}"
    if run_with_timeout make "tests/${test}" >"${logfile}" 2>&1; then
        echo PASS > "${result}"
        log "PASS:  ${test}"
    else
        local code=$?
        if [ "${code}" -eq 124 ]; then
            echo TIMEOUT > "${result}"
            log "TIMEOUT: ${test} (>${TIMEOUT}s) — see ${logfile}"
        else
            echo FAIL > "${result}"
            log "FAIL:  ${test} — see ${logfile}"
        fi
    fi
}
export -f run_test log
export LOGDIR TIMEOUT DOCKER PGVERSION

run_tier() {
    local tier_name=$1
    shift
    local tests=("$@")
    local pids=()
    local slot=0

    log "=== ${tier_name}: ${#tests[@]} tests, ${JOBS} parallel slots ==="

    for test in "${tests[@]}"; do
        # Skip tests with no directory
        if [ ! -d "tests/${test}" ]; then
            log "SKIP:  ${test} (no directory)"
            echo SKIP > "${LOGDIR}/${test}.result"
            continue
        fi

        # Throttle to JOBS parallel slots
        while [ "${#pids[@]}" -ge "${JOBS}" ]; do
            local new_pids=()
            for pid in "${pids[@]}"; do
                if kill -0 "${pid}" 2>/dev/null; then
                    new_pids+=("${pid}")
                fi
            done
            pids=("${new_pids[@]+"${new_pids[@]}"}")
            [ "${#pids[@]}" -ge "${JOBS}" ] && sleep 1
        done

        run_test "${test}" &
        pids+=($!)
    done

    # Wait for remaining jobs
    for pid in "${pids[@]+"${pids[@]}"}"; do
        wait "${pid}" 2>/dev/null || true
    done
}

# ── Build ────────────────────────────────────────────────────────────────────

log "=== BUILD: pgcopydb image for PG${PGVERSION} ==="
DOCKER=${DOCKER} PGVERSION=${PGVERSION} make build

# ── Tier 2 ───────────────────────────────────────────────────────────────────

run_tier "TIER 2 (smoke)" "${TIER2[@]}"

tier2_failed=0
for t in "${TIER2[@]}"; do
    result=$(cat "${LOGDIR}/${t}.result" 2>/dev/null || echo MISSING)
    [ "${result}" != "PASS" ] && [ "${result}" != "SKIP" ] && tier2_failed=1
done

if [ "${tier2_failed}" -eq 1 ]; then
    log "=== TIER 2 FAILED — skipping integration tests ==="
    # Mark all tier 3 tests as SKIP so they don't appear as FAIL in summary
    for t in "${TIER3[@]}"; do
        echo SKIP > "${LOGDIR}/${t}.result"
    done
else
    # ── Tier 3 ───────────────────────────────────────────────────────────────
    run_tier "TIER 3 (integration)" "${TIER3[@]}"
fi

# ── Summary ──────────────────────────────────────────────────────────────────

pass=(); fail=(); timeout=(); skip=()
all_tests=("${TIER2[@]}" "${TIER3[@]}")

for t in "${all_tests[@]}"; do
    result=$(cat "${LOGDIR}/${t}.result" 2>/dev/null || echo MISSING)
    case "${result}" in
        PASS)    pass+=("${t}") ;;
        FAIL)    fail+=("${t}") ;;
        TIMEOUT) timeout+=("${t}") ;;
        SKIP)    skip+=("${t}") ;;
        *)       fail+=("${t}") ;;
    esac
done

echo ""
echo "══════════════════════════════════════════════════"
echo " Results: PG${PGVERSION}"
echo "══════════════════════════════════════════════════"
printf " PASS:    %d\n" "${#pass[@]}"
printf " FAIL:    %d\n" "${#fail[@]}"
printf " TIMEOUT: %d\n" "${#timeout[@]}"
printf " SKIP:    %d\n" "${#skip[@]}"
echo "──────────────────────────────────────────────────"

for t in "${fail[@]+"${fail[@]}"}"; do
    printf " FAIL:    %s\n" "${t}"
    printf "          log: %s/%s.log\n" "${LOGDIR}" "${t}"
done
for t in "${timeout[@]+"${timeout[@]}"}"; do
    printf " TIMEOUT: %s\n" "${t}"
    printf "          log: %s/%s.log\n" "${LOGDIR}" "${t}"
done

echo "══════════════════════════════════════════════════"

[ "${#fail[@]}" -eq 0 ] && [ "${#timeout[@]}" -eq 0 ]
