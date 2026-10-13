#! /bin/bash

set -x
set -e

# This script expects the following environment variables to be set:
#
#  - PGCOPYDB_SOURCE_PGURI
#  - PGCOPYDB_TARGET_PGURI
#  - PGCOPYDB_TABLE_JOBS
#  - PGCOPYDB_INDEX_JOBS

# make sure source and target databases are ready
pgcopydb ping

# import.sql reassigns one large object to a dedicated, non-connecting role to
# exercise ownership preservation.  The role must exist on both ends: pgcopydb
# does not copy roles as part of `dump schema` / `restore pre-data`, so we
# create it explicitly on each side (this mirrors real deployments where the
# owner role is provisioned on the target cluster ahead of the migration).
psql -d ${PGCOPYDB_SOURCE_PGURI} -1 -c "create role blobowner with login;"
psql -d ${PGCOPYDB_TARGET_PGURI} -1 -c "create role blobowner with login;"

# a role with a name that requires identifier quoting, used as an ACL grantee
psql -d ${PGCOPYDB_SOURCE_PGURI} -1 -c 'create role "blob user";'
psql -d ${PGCOPYDB_TARGET_PGURI} -1 -c 'create role "blob user";'

psql -d ${PGCOPYDB_SOURCE_PGURI} -1 -f /usr/src/pgcopydb/import.sql

# decorate large objects with ACLs and a comment on the source database, so
# that we can check that pgcopydb copies that metadata over: loid1 gets
# explicit grants and a comment, loid2 gets an empty (non-NULL) ACL
loid1=$(psql -AXqt -d ${PGCOPYDB_SOURCE_PGURI} -c 'select oid from pg_largeobject_metadata order by oid limit 1')
loid2=$(psql -AXqt -d ${PGCOPYDB_SOURCE_PGURI} -c 'select oid from pg_largeobject_metadata order by oid offset 1 limit 1')

psql -d ${PGCOPYDB_SOURCE_PGURI} -1 <<EOF
grant select on large object ${loid1} to "blob user" with grant option;
grant update on large object ${loid1} to public;
comment on large object ${loid1} is 'it''s a comment with quotes';
alter large object ${loid2} owner to blobowner;
revoke all on large object ${loid2} from blobowner;
-- a grant performed by a non-owner grantor (WITH GRANT OPTION chain): the
-- grantor must be preserved on the target, as shown in lomacl by the final
-- metadata diff (entry "blobowner=r/blob user")
set role "blob user";
grant select on large object ${loid1} to blobowner;
reset role;
EOF

# Save info of blobs on the source to compare against the target after migration
# for validation. We are doing this because we are going to insert some blobs
# after taking snapshot and ensure we don't migrate them. The owner column also
# proves pg_largeobject_metadata.lomowner is carried across (see import.sql).
SQL="select m.oid, pg_catalog.pg_get_userbyid(m.lomowner) as owner, count(l.data) as parts, sum(length(l.data)) as size from pg_largeobject_metadata m join pg_largeobject l on l.loid = m.oid group by m.oid, m.lomowner order by m.oid;"

# also compare the large object ACLs and comments between source and target
MSQL="select m.oid, m.lomacl::text, obj_description(m.oid, 'pg_largeobject') as description from pg_largeobject_metadata m order by m.oid;"

psql -d ${PGCOPYDB_SOURCE_PGURI} -1 -c "${SQL}" > /tmp/source.lo
psql -d ${PGCOPYDB_SOURCE_PGURI} -1 -c "${MSQL}" > /tmp/source.lometadata

psql -d ${PGCOPYDB_SOURCE_PGURI} -1 -c 'table pg_largeobject_metadata'
psql -d ${PGCOPYDB_TARGET_PGURI} -1 -c 'table pg_largeobject_metadata'

#
# pgcopydb uses the environment variables
#
# we need to export a snapshot, and keep it while the indivual steps are
# running, one at a time

coproc ( psql -At -d ${PGCOPYDB_SOURCE_PGURI} 2>&1 )

echo 'begin;' >&"${COPROC[1]}"
read x <&"${COPROC[0]}"

echo 'set transaction isolation level serializable, read only, deferrable;' >&"${COPROC[1]}"
read x <&"${COPROC[0]}"

echo 'select pg_export_snapshot();' >&"${COPROC[1]}"
read sn <&"${COPROC[0]}"

export PGCOPYDB_SNAPSHOT="${sn}"

# with a PGCOPYDB_SNAPSHOT in the environment, no need for --resume etc.
echo snapshot ${PGCOPYDB_SNAPSHOT}

# Insert some more blobs. This is to ensure we don't restore blobs on target
# that weren't included in snapshot.
psql -d ${PGCOPYDB_SOURCE_PGURI} -1 -f /usr/src/pgcopydb/import.sql
psql -d ${PGCOPYDB_SOURCE_PGURI} -1 -c 'table pg_largeobject_metadata'

pgcopydb dump schema --snapshot "${sn}"
pgcopydb restore pre-data --resume

# the schema dump uses pg_dump --no-blobs, so pgcopydb restore pre-data does
# not create any large object: they are created by pgcopydb copy blobs
count=$(psql -AXqt -d ${PGCOPYDB_TARGET_PGURI} -c 'select count(*) from pg_largeobject_metadata')
test "${count}" = "0"

pgcopydb copy blobs --large-objects-jobs 2 --resume

# check that the first copy actually transferred the large objects data:
# the data on the target must already match the source snapshot
psql -d ${PGCOPYDB_TARGET_PGURI} -1 -c "${SQL}" > /tmp/target-first.lo
diff /tmp/source.lo /tmp/target-first.lo

# by default, large objects that already exist on the target are skipped
# entirely: alter a comment on the target, run the copy again, and check
# that our local change is still there (the large object was not copied)
psql -d ${PGCOPYDB_TARGET_PGURI} -1 -c "comment on large object ${loid1} is 'locally modified';"

# in the same run, exercise the mixed resume case: replace one large object
# with an empty one on the target, simulating a large object pre-created by
# a schema restore (as previous pgcopydb versions did); an existing large
# object without any data must not be mistaken for an already-copied one,
# and must be copied again by the same run that skips the other ones
psql -d ${PGCOPYDB_TARGET_PGURI} -1 -c "select lo_unlink(${loid2}); select lo_create(${loid2});"

pgcopydb copy blobs --large-objects-jobs 2 --resume

comment=$(psql -AXqt -d ${PGCOPYDB_TARGET_PGURI} -c "select obj_description(${loid1}, 'pg_largeobject')")
test "${comment}" = "locally modified"

# the emptied large object has been copied again: its data matches the
# source and its ownership has been restored
srcsize=$(psql -AXqt -d ${PGCOPYDB_SOURCE_PGURI} -c "select coalesce(sum(length(data)), 0) from pg_largeobject where loid = ${loid2}")
dstsize=$(psql -AXqt -d ${PGCOPYDB_TARGET_PGURI} -c "select coalesce(sum(length(data)), 0) from pg_largeobject where loid = ${loid2}")
test "${srcsize}" = "${dstsize}"

owner=$(psql -AXqt -d ${PGCOPYDB_TARGET_PGURI} -c "select pg_get_userbyid(lomowner) from pg_largeobject_metadata where oid = ${loid2}")
test "${owner}" = "blobowner"

# with --drop-if-exists, existing large objects are dropped and copied all
# over again: the source comment is restored, as checked by the final
# metadata diff below
pgcopydb copy blobs --large-objects-jobs 2 --resume --drop-if-exists

pgcopydb restore post-data --resume

pgcopydb list progress --summary

echo 'commit;' >&"${COPROC[1]}"
echo '\q' >&"${COPROC[1]}"

wait ${COPROC_PID}

psql -d ${PGCOPYDB_TARGET_PGURI} -1 -c "${SQL}" > /tmp/target.lo
psql -d ${PGCOPYDB_TARGET_PGURI} -1 -c "${MSQL}" > /tmp/target.lometadata

diff /tmp/source.lo /tmp/target.lo
diff /tmp/source.lometadata /tmp/target.lometadata
