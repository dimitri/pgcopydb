#!/usr/bin/env bash
# run-cdc-tests.sh — non-interactive CDC test runner with log sampling
#
# Usage:
#   ./tests/run-cdc-tests.sh [test1 test2 ...]          # run specific tests
#   ./tests/run-cdc-tests.sh                            # run all CDC tests
#   SKIP_BUILD=1 ./tests/run-cdc-tests.sh cdc-wal2json  # skip docker build
#   PGVERSION=16 ./tests/run-cdc-tests.sh               # explicit PG version
#
# Output:
#   /tmp/pgcopydb-test-<timestamp>/
#     build.log         — pgcopydb + pagila image build output
#     <test>-run.log    — full docker compose output per test
#     <test>-sample.log — last 60 lines + key log lines (errors/notices/PASS)
#     summary.txt       — pass/fail counts and result per test

set -euo pipefail

PGVERSION="${PGVERSION:-16}"
SKIP_BUILD="${SKIP_BUILD:-0}"
TIMEOUT="${TIMEOUT:-300}"       # seconds per test
TESTS_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(dirname "$TESTS_DIR")"

# Determine test list
ALL_CDC_TESTS=(
    cdc-endpos-mid-txn
    cdc-wal2json
    cdc-transform-apply
    cdc-low-level
    cdc-test-decoding
    cdc-replica-identity-index
    cdc-partitioned-target
)

if [[ $# -gt 0 ]]; then
    TESTS=("$@")
else
    TESTS=("${ALL_CDC_TESTS[@]}")
fi

LOGDIR="/tmp/pgcopydb-test-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$LOGDIR"

SUMMARY="$LOGDIR/summary.txt"
PASS=0
FAIL=0
SKIP=0

log() { echo "[$(date '+%H:%M:%S')] $*"; }
log_section() {
    echo ""
    echo "────────────────────────────────────────────────────────────────────────"
    echo "$*"
    echo "────────────────────────────────────────────────────────────────────────"
}

# Cross-platform timeout: prefer GNU timeout, then gtimeout (Homebrew), then
# fall back to running without a timeout (tests will still fail on error).
if command -v timeout &>/dev/null; then
    TIMEOUT_CMD="timeout"
elif command -v gtimeout &>/dev/null; then
    TIMEOUT_CMD="gtimeout"
else
    TIMEOUT_CMD=""
    log "WARNING: no timeout command found; tests will run without a time limit"
fi

run_with_timeout() {
    local secs="$1"
    shift
    if [[ -n "$TIMEOUT_CMD" ]]; then
        "$TIMEOUT_CMD" "$secs" "$@"
    else
        "$@"
    fi
}

# ── Build phase ────────────────────────────────────────────────────────────────
if [[ "$SKIP_BUILD" != "1" ]]; then
    log_section "Building pgcopydb Docker image (source → binary in container)"
    BUILD_LOG="$LOGDIR/build-pgcopydb.log"

    if docker build \
            --build-arg PGVERSION="$PGVERSION" \
            -t pgcopydb \
            -f "$ROOT_DIR/Dockerfile" \
            "$ROOT_DIR" \
            >"$BUILD_LOG" 2>&1; then
        log "pgcopydb image built OK"
    else
        log "ERROR: pgcopydb image build FAILED — see $BUILD_LOG"
        tail -20 "$BUILD_LOG"
        exit 1
    fi

    log_section "Building pagila base image (embeds pgcopydb binary)"
    PAGILA_LOG="$LOGDIR/build-pagila.log"

    if docker build \
            --build-arg PGVERSION="$PGVERSION" \
            -t pagila \
            -f "$TESTS_DIR/Dockerfile.pagila" \
            "$TESTS_DIR" \
            >"$PAGILA_LOG" 2>&1; then
        log "pagila image built OK"
    else
        log "ERROR: pagila image build FAILED — see $PAGILA_LOG"
        tail -20 "$PAGILA_LOG"
        exit 1
    fi
else
    log "SKIP_BUILD=1: skipping Docker image rebuild"
fi

# ── Helper: extract key lines from a log ───────────────────────────────────────
extract_sample() {
    local log_file="$1"
    local sample_file="$2"
    local test_name="$3"

    {
        echo "=== $test_name: last 60 lines ==="
        tail -60 "$log_file"
        echo ""
        echo "=== $test_name: NOTICE / WARNING / ERROR / FATAL lines ==="
        grep -iE "(NOTICE|WARNING|ERROR|FATAL|PASS|FAIL|assert|mid-transaction|endpos|reached)" \
             "$log_file" 2>/dev/null | tail -40 || true
    } >"$sample_file"
}

# ── Test runner ─────────────────────────────────────────────────────────────────
log_section "Running ${#TESTS[@]} test(s): ${TESTS[*]}"

{
    echo "pgcopydb CDC test run — $(date)"
    echo "PGVERSION=$PGVERSION  TIMEOUT=${TIMEOUT}s  SKIP_BUILD=$SKIP_BUILD"
    echo "Tests: ${TESTS[*]}"
    echo ""
} >"$SUMMARY"

for TEST in "${TESTS[@]}"; do
    TEST_DIR="$TESTS_DIR/$TEST"

    if [[ ! -d "$TEST_DIR" ]]; then
        log "SKIP $TEST (directory not found: $TEST_DIR)"
        echo "SKIP  $TEST  (directory not found)" >>"$SUMMARY"
        ((SKIP++)) || true
        continue
    fi

    RUN_LOG="$LOGDIR/${TEST}-run.log"
    SAMPLE_LOG="$LOGDIR/${TEST}-sample.log"

    log_section "Starting test: $TEST  (timeout=${TIMEOUT}s)"

    # Tear down any leftover containers and volumes from a previous run.
    # The -v flag removes named volumes so each run starts with a clean state
    # (previously cdc-wal2json required a separate fix-volumes step for this).
    (cd "$TEST_DIR" && docker compose down -v --remove-orphans 2>/dev/null) || true

    # Build test-specific images (only layers changed since pagila; fast with cache)
    log "Building test containers for $TEST..."
    if ! (cd "$TEST_DIR" && \
          PGVERSION="$PGVERSION" docker compose build --quiet >>"$RUN_LOG" 2>&1); then
        log "ERROR: docker compose build failed for $TEST"
        extract_sample "$RUN_LOG" "$SAMPLE_LOG" "$TEST"
        echo "FAIL  $TEST  (build failed)" >>"$SUMMARY"
        ((FAIL++)) || true
        (cd "$TEST_DIR" && docker compose down --remove-orphans 2>/dev/null) || true
        continue
    fi

    # Run the test with a timeout
    START=$(date +%s)
    log "Running $TEST..."

    TEST_RC=0
    run_with_timeout "$TIMEOUT" \
        bash -c "cd '$TEST_DIR' && PGVERSION='$PGVERSION' docker compose run --rm test" \
        >>"$RUN_LOG" 2>&1 || TEST_RC=$?

    END=$(date +%s)
    ELAPSED=$((END - START))

    # Clean up containers and volumes regardless of outcome
    (cd "$TEST_DIR" && docker compose down -v --remove-orphans 2>/dev/null) || true

    extract_sample "$RUN_LOG" "$SAMPLE_LOG" "$TEST"

    if [[ $TEST_RC -eq 124 ]]; then
        log "TIMEOUT $TEST after ${ELAPSED}s"
        echo "FAIL  $TEST  (timeout after ${ELAPSED}s)" >>"$SUMMARY"
        ((FAIL++)) || true
    elif [[ $TEST_RC -ne 0 ]]; then
        log "FAIL $TEST (exit $TEST_RC, ${ELAPSED}s)"
        echo "FAIL  $TEST  (exit $TEST_RC, ${ELAPSED}s)" >>"$SUMMARY"
        ((FAIL++)) || true
        # Print last 30 lines for quick diagnosis
        echo ""
        echo "  ── last 30 lines of $TEST output ──"
        tail -30 "$RUN_LOG" | sed 's/^/  /'
        echo ""
    else
        log "PASS $TEST (${ELAPSED}s)"
        echo "PASS  $TEST  (${ELAPSED}s)" >>"$SUMMARY"
        ((PASS++)) || true
    fi
done

# ── Summary ────────────────────────────────────────────────────────────────────
log_section "Test summary"
cat "$SUMMARY"
echo ""
{
    echo ""
    echo "Results: PASS=$PASS  FAIL=$FAIL  SKIP=$SKIP"
    echo "Logs:    $LOGDIR"
} | tee -a "$SUMMARY"

log "Logs saved to $LOGDIR"

[[ $FAIL -eq 0 ]]
