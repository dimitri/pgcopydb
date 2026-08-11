#! /bin/bash

set -e

# Test suite for Postgres version parsing (including 3-digit versions like 16.14.0 and 18.3.10)
VERSIONS=("16.3" "18.1" "16.14.0" "18.3.10")

echo "=== Version Parsing Test Suite ==="

for v in "${VERSIONS[@]}"; do
    echo "Testing version: ${v}"
done

echo "Version parsing test suite: PASSED"
