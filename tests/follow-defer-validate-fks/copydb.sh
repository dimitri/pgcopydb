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

# ensure TMPDIR is writable by the docker user
sudo mkdir -p ${TMPDIR}
sudo chown -R `whoami` ${TMPDIR}

# make sure source and target databases are ready
pgcopydb ping

# Load pagila schema and data on the source
psql -o /tmp/s.out -d ${PGCOPYDB_SOURCE_PGURI} -1 -f /usr/src/pagila/pagila-schema.sql
psql -o /tmp/d.out -d ${PGCOPYDB_SOURCE_PGURI} -1 -f /usr/src/pagila/pagila-data.sql

# alter the pagila schema to allow capturing DDLs without pkey
psql -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/ddl.sql

#
# Count FK constraints on the source. pagila has FKs on both ordinary tables
# and on the partitioned "payment" table, which is exactly the shape that
# exercises the NOT VALID + partitioned-table interaction.
#
src_fk_count=$(psql -AtX -d ${PGCOPYDB_SOURCE_PGURI} -c \
  "SELECT count(*)
     FROM pg_constraint
    WHERE contype = 'f'
      AND connamespace = 'public'::regnamespace")

echo "=== Source has ${src_fk_count} foreign key constraints ==="

if [ "${src_fk_count}" -lt 1 ]; then
    echo "ERROR: expected at least 1 FK on the source, found ${src_fk_count}"
    exit 1
fi

#
# Run the common "huge customer" flag combination:
#
#   clone --follow --defer-indexes --defer-validate-fks
#         --split-tables-larger-than ... --plugin wal2json
#
# Use a small split threshold so the larger pagila tables are actually
# split into parts (key-based on their integer PKs), exercising the
# split-tables path alongside deferred indexes and deferred FK validation.
#
# The inject sidecar injects DML (rental/payment INSERT/UPDATE/DELETE, which
# all carry FK relationships) during CDC and then sets endpos. If CDC replay
# against NOT VALID constraints were unsafe, those changes would fail to apply.
#
pgcopydb clone \
         --follow \
         --defer-indexes \
         --defer-validate-fks \
         --split-tables-larger-than 50kB \
         --split-max-parts 4 \
         --plugin wal2json \
         --notice

# cleanup
pgcopydb stream sentinel get

# make sure the inject service has had time to see the final sentinel values
sleep 2
pgcopydb stream cleanup

echo ""
echo "=== Clone --follow completed, verifying results ==="
echo ""

#
# Verify data matches between source and target (proves CDC replay applied
# the injected DML on top of NOT VALID constraints).
#
sql="select count(*), sum(amount) from payment"

src_result=`psql -AtX -d ${PGCOPYDB_SOURCE_PGURI} -c "${sql}"`
tgt_result=`psql -AtX -d ${PGCOPYDB_TARGET_PGURI} -c "${sql}"`

echo "Source payment: ${src_result}"
echo "Target payment: ${tgt_result}"

if [ "${src_result}" != "${tgt_result}" ]; then
    echo "ERROR: Source and target payment data do not match!"
    exit 1
fi

#
# All FK constraints must be present on the target, and every one of them
# must be NOT VALID (convalidated = false) because of --defer-validate-fks.
#
tgt_fk_count=$(psql -AtX -d ${PGCOPYDB_TARGET_PGURI} -c \
  "SELECT count(*)
     FROM pg_constraint
    WHERE contype = 'f'
      AND connamespace = 'public'::regnamespace")

tgt_fk_valid=$(psql -AtX -d ${PGCOPYDB_TARGET_PGURI} -c \
  "SELECT count(*)
     FROM pg_constraint
    WHERE contype = 'f'
      AND connamespace = 'public'::regnamespace
      AND convalidated")

tgt_fk_notvalid=$(psql -AtX -d ${PGCOPYDB_TARGET_PGURI} -c \
  "SELECT count(*)
     FROM pg_constraint
    WHERE contype = 'f'
      AND connamespace = 'public'::regnamespace
      AND NOT convalidated")

echo "Source FK count:        ${src_fk_count}"
echo "Target FK count:        ${tgt_fk_count}"
echo "Target FK NOT VALID:    ${tgt_fk_notvalid}"
echo "Target FK validated:    ${tgt_fk_valid}"

if [ "${tgt_fk_count}" -lt "${src_fk_count}" ]; then
    echo "ERROR: Target has fewer FK constraints (${tgt_fk_count}) than source (${src_fk_count})!"
    exit 1
fi

#
# Every FK on the target must be NOT VALID with --defer-validate-fks. A
# non-zero "validated" count means some FK escaped the deferral (for example
# a partitioned-table FK that could not be created NOT VALID) and would have
# incurred the validation scan we are trying to avoid.
#
if [ "${tgt_fk_valid}" != "0" ]; then
    echo "ERROR: ${tgt_fk_valid} FK constraint(s) were created VALID despite"
    echo "  --defer-validate-fks. They escaped deferral and ran a validation"
    echo "  scan. Listing them:"
    psql -d ${PGCOPYDB_TARGET_PGURI} -c \
      "SELECT conrelid::regclass AS tbl, conname
         FROM pg_constraint
        WHERE contype = 'f'
          AND connamespace = 'public'::regnamespace
          AND convalidated"
    exit 1
fi

#
# Enforcement must still hold on a NOT VALID constraint: a write that
# violates a FK must be rejected (this is what keeps CDC replay safe).
#
set +e
result=$(psql -d ${PGCOPYDB_TARGET_PGURI} -c \
  "INSERT INTO rental (rental_date, inventory_id, customer_id, staff_id, last_update)
   VALUES ('2022-06-01', 999999999, 999999999, 999999999, '2022-06-01')" 2>&1)
set -e

if echo "${result}" | grep -q "violates foreign key constraint"; then
    echo "Violating write correctly rejected on NOT VALID constraint."
else
    echo "ERROR: NOT VALID constraint should still reject violating writes!"
    echo "psql output: ${result}"
    exit 1
fi

#
# Deferred validation must be completable later: validating one known-clean
# FK should succeed and flip convalidated to true.
#
psql -d ${PGCOPYDB_TARGET_PGURI} -c \
    "ALTER TABLE rental VALIDATE CONSTRAINT rental_customer_id_fkey"

post_validate=$(psql -AtX -d ${PGCOPYDB_TARGET_PGURI} -c \
  "SELECT convalidated
     FROM pg_constraint
    WHERE conname = 'rental_customer_id_fkey'")

if [ "${post_validate}" != "t" ]; then
    echo "ERROR: VALIDATE CONSTRAINT should have set convalidated=t"
    exit 1
fi

echo ""
echo "follow-defer-validate-fks test: PASSED"
echo ""
echo "clone --follow --defer-indexes --defer-validate-fks --split-tables-larger-than:"
echo "  - ${tgt_fk_count} FK constraints created, all NOT VALID on target"
echo "  - data consistent after CDC replay of injected DML"
echo "  - violating writes still rejected on NOT VALID constraints"
echo "  - manual VALIDATE CONSTRAINT succeeds against clean data"
