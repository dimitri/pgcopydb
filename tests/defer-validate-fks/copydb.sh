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

# make sure source and target databases are ready
pgcopydb ping

#
# Build a source schema with two FK shapes:
#
#  1. A pair of tables with clean data and a validating FK on source
#     (convalidated = true). With --defer-validate-fks we expect this to
#     land on the target as NOT VALID (the validation scan is skipped).
#
#  2. A pair of tables where the FK was added NOT VALID on source
#     (convalidated = false). It should remain NOT VALID on the target
#     regardless of the flag.
#
psql -d ${PGCOPYDB_SOURCE_PGURI} <<'SQL'

CREATE TABLE clean_parent (
    id serial PRIMARY KEY,
    name text
);

CREATE TABLE clean_child (
    id serial PRIMARY KEY,
    parent_id integer REFERENCES clean_parent(id)
);

INSERT INTO clean_parent VALUES (1, 'p1'), (2, 'p2');
INSERT INTO clean_child VALUES (1, 1), (2, 2);

-- Source-side NOT VALID FK with a pre-existing orphan row (999 has no parent)
CREATE TABLE legacy_parent (
    id serial PRIMARY KEY,
    name text
);

CREATE TABLE legacy_child (
    id serial PRIMARY KEY,
    parent_id integer
);

INSERT INTO legacy_parent VALUES (1, 'lp1');
INSERT INTO legacy_child VALUES (1, 1), (2, 999);

ALTER TABLE legacy_child
    ADD CONSTRAINT legacy_child_parent_id_fkey
    FOREIGN KEY (parent_id) REFERENCES legacy_parent(id)
    NOT VALID;

-- Partitioned table with a PARENT-LEVEL FK. PostgreSQL refuses to create a
-- NOT VALID foreign key declared on a partitioned table, so
-- --defer-validate-fks must SKIP this constraint (and crucially must NOT
-- abort the whole migration over it).
CREATE TABLE part_child (
    id integer,
    parent_id integer,
    created date NOT NULL
) PARTITION BY RANGE (created);

CREATE TABLE part_child_2022 PARTITION OF part_child
    FOR VALUES FROM ('2022-01-01') TO ('2023-01-01');
CREATE TABLE part_child_2023 PARTITION OF part_child
    FOR VALUES FROM ('2023-01-01') TO ('2024-01-01');

INSERT INTO part_child VALUES (1, 1, '2022-06-01'), (2, 2, '2023-06-01');

ALTER TABLE part_child
    ADD CONSTRAINT part_child_parent_id_fkey
    FOREIGN KEY (parent_id) REFERENCES clean_parent(id);

SQL

#
# Sanity-check the source state: clean FK is convalidated, legacy FK is not.
#
src_clean_state=$(psql -AtX -d ${PGCOPYDB_SOURCE_PGURI} -c \
  "SELECT convalidated FROM pg_constraint WHERE conname = 'clean_child_parent_id_fkey'")
src_legacy_state=$(psql -AtX -d ${PGCOPYDB_SOURCE_PGURI} -c \
  "SELECT convalidated FROM pg_constraint WHERE conname = 'legacy_child_parent_id_fkey'")

echo "=== Source clean FK convalidated:  ${src_clean_state} (expect t) ==="
echo "=== Source legacy FK convalidated: ${src_legacy_state} (expect f) ==="

if [ "${src_clean_state}" != "t" ] || [ "${src_legacy_state}" != "f" ]; then
    echo "ERROR: source FK setup is wrong"
    exit 1
fi

#
# Run pgcopydb clone with --defer-validate-fks. Every FK should land on
# the target as NOT VALID; pgcopydb should not run any validating seqscans.
#
pgcopydb clone --notice --defer-validate-fks

echo ""
echo "=== Clone completed, verifying results ==="
echo ""

#
# Both FKs should be NOT VALID on the target.
#
tgt_clean_state=$(psql -AtX -d ${PGCOPYDB_TARGET_PGURI} -c \
  "SELECT convalidated FROM pg_constraint WHERE conname = 'clean_child_parent_id_fkey'")
tgt_legacy_state=$(psql -AtX -d ${PGCOPYDB_TARGET_PGURI} -c \
  "SELECT convalidated FROM pg_constraint WHERE conname = 'legacy_child_parent_id_fkey'")

echo "Target clean FK convalidated:  ${tgt_clean_state} (expect f)"
echo "Target legacy FK convalidated: ${tgt_legacy_state} (expect f)"

if [ "${tgt_clean_state}" != "f" ]; then
    echo "ERROR: clean FK should be NOT VALID on target with --defer-validate-fks"
    exit 1
fi

if [ "${tgt_legacy_state}" != "f" ]; then
    echo "ERROR: source-NOT-VALID FK should remain NOT VALID on target"
    exit 1
fi

#
# Data should still be copied in full, including the pre-existing orphan
# row on the source-NOT-VALID table.
#
tgt_clean_rows=$(psql -AtX -d ${PGCOPYDB_TARGET_PGURI} -c "SELECT count(*) FROM clean_child")
tgt_legacy_rows=$(psql -AtX -d ${PGCOPYDB_TARGET_PGURI} -c "SELECT count(*) FROM legacy_child")

if [ "${tgt_clean_rows}" != "2" ] || [ "${tgt_legacy_rows}" != "2" ]; then
    echo "ERROR: row count mismatch (clean=${tgt_clean_rows}, legacy=${tgt_legacy_rows})"
    exit 1
fi

#
# Partitioned-table FK behavior under --defer-validate-fks. The crucial
# property — and the one that holds across PostgreSQL versions — is that the
# clone did NOT abort over the partitioned table (we reached this point at
# all) and that NO foreign key on the partitioned hierarchy was validated
# (no validation scan ran, which is the whole point of the flag).
#
# The exact shape differs by version:
#   - PG < 18: PostgreSQL refuses NOT VALID on the partitioned parent, so
#     pgcopydb skips the parent-level row and creates the per-partition rows
#     NOT VALID directly.
#   - PG >= 18: the parent-level NOT VALID FK is supported, so it is created
#     directly (and propagates NOT VALID to the partitions).
#
# Either way: at least one NOT VALID FK row exists for the hierarchy
# (enforcement is in place) and none are validated.
#
part_fk_total=$(psql -AtX -d ${PGCOPYDB_TARGET_PGURI} -c \
  "SELECT count(*)
     FROM pg_constraint
    WHERE conname = 'part_child_parent_id_fkey'
      AND conrelid IN ('part_child'::regclass,
                       'part_child_2022'::regclass,
                       'part_child_2023'::regclass)")

part_fk_validated=$(psql -AtX -d ${PGCOPYDB_TARGET_PGURI} -c \
  "SELECT count(*)
     FROM pg_constraint
    WHERE conname = 'part_child_parent_id_fkey'
      AND conrelid IN ('part_child'::regclass,
                       'part_child_2022'::regclass,
                       'part_child_2023'::regclass)
      AND convalidated")

tgt_part_rows=$(psql -AtX -d ${PGCOPYDB_TARGET_PGURI} -c "SELECT count(*) FROM part_child")

echo "Target partitioned-hierarchy FK rows: ${part_fk_total} (expect >= 1)"
echo "  of which validated:                 ${part_fk_validated} (expect 0)"
echo "Target part_child rows:               ${tgt_part_rows} (expect 2)"

if [ "${part_fk_total}" -lt 1 ]; then
    echo "ERROR: expected at least one NOT VALID FK on the partitioned hierarchy"
    exit 1
fi

if [ "${part_fk_validated}" != "0" ]; then
    echo "ERROR: no partitioned-hierarchy FK should be validated under defer"
    echo "  (a validation scan ran, which --defer-validate-fks must avoid)"
    exit 1
fi

if [ "${tgt_part_rows}" != "2" ]; then
    echo "ERROR: partitioned table data was not fully copied"
    exit 1
fi

#
# This is the invariant that makes CDC replay safe on top of a NOT VALID
# constraint: NOT VALID only skips the scan of pre-existing rows, it still
# enforces referential integrity on every new write. A change replayed from
# the source already satisfies the source's constraint, so it will satisfy
# the target's NOT VALID constraint too.
#
# 1. A write that violates the FK must be rejected.
#
set +e
result=$(psql -d ${PGCOPYDB_TARGET_PGURI} -c \
  "INSERT INTO clean_child (id, parent_id) VALUES (99, 9999)" 2>&1)
set -e

if echo "${result}" | grep -q "violates foreign key constraint"; then
    echo "Violating write correctly rejected on NOT VALID constraint."
else
    echo "ERROR: NOT VALID constraint should still reject violating writes!"
    echo "psql output: ${result}"
    exit 1
fi

#
# 2. A write that satisfies the FK must succeed (the shape of a normal CDC
#    replayed INSERT).
#
psql -d ${PGCOPYDB_TARGET_PGURI} -c \
    "INSERT INTO clean_child (id, parent_id) VALUES (100, 1)"

ok_rows=$(psql -AtX -d ${PGCOPYDB_TARGET_PGURI} -c \
  "SELECT count(*) FROM clean_child WHERE id = 100")

if [ "${ok_rows}" != "1" ]; then
    echo "ERROR: valid write should be accepted on NOT VALID constraint"
    exit 1
fi

# undo the inserted row so the manual VALIDATE below has clean data
psql -d ${PGCOPYDB_TARGET_PGURI} -c "DELETE FROM clean_child WHERE id = 100"

#
# A manual VALIDATE CONSTRAINT on the clean FK should succeed (data is
# clean by construction) and flip convalidated to true. This proves the
# deferred validation path works for callers that want to validate later.
#
psql -d ${PGCOPYDB_TARGET_PGURI} -c \
    "ALTER TABLE clean_child VALIDATE CONSTRAINT clean_child_parent_id_fkey"

post_validate_state=$(psql -AtX -d ${PGCOPYDB_TARGET_PGURI} -c \
  "SELECT convalidated FROM pg_constraint WHERE conname = 'clean_child_parent_id_fkey'")

if [ "${post_validate_state}" != "t" ]; then
    echo "ERROR: VALIDATE CONSTRAINT should have set convalidated=t"
    exit 1
fi

echo ""
echo "defer-validate-fks test: PASSED"
echo ""
echo "  - Clean FK landed on target as NOT VALID per --defer-validate-fks"
echo "  - Source-NOT-VALID FK stayed NOT VALID on target"
echo "  - Data was copied fully (including pre-existing orphan rows)"
echo "  - Violating writes still rejected on NOT VALID constraints"
echo "  - Valid writes (CDC-shaped) still accepted"
echo "  - Manual VALIDATE CONSTRAINT succeeds against clean data"
