#! /bin/bash

set -x
#set -e

# This script expects the following environment variables to be set:
#
#  - PGCOPYDB_SOURCE_PGURI
#  - PGCOPYDB_TARGET_PGURI
#  - PGCOPYDB_TABLE_JOBS
#  - PGCOPYDB_INDEX_JOBS

# make sure source and target databases are ready
pgcopydb ping

psql -o /tmp/s.out -d ${PGCOPYDB_SOURCE_PGURI} -1 -f /usr/src/pagila/pagila-schema.sql
psql -o /tmp/d.out -d ${PGCOPYDB_SOURCE_PGURI} -1 -f /usr/src/pagila/pagila-data.sql

# alter the pagila schema to allow capturing DDLs without pkey
psql -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/ddl.sql

# pgcopydb clone uses the environment variables
pgcopydb clone --follow --plugin wal2json --notice

# Query the SQLite CDC database to verify output/replay tables exist
db="${TMPDIR}/pgcopydb/cdc/pgcopydb/00000001-"*.db
if [ -f "$db" ]; then
  sqlite3 "$db" <<EOF
select id, action, xid, lsn, substring(message, 1, 48) from output limit 10;

select hash, sql from stmt limit 5;

select id, action, xid, lsn, endlsn, stmt_hash, stmt_args from replay limit 10;
EOF
else
  echo "CDC database not found at ${TMPDIR}/pgcopydb/cdc/pgcopydb/"
fi

# cleanup
pgcopydb stream sentinel get

# make sure the inject service has had time to see the final sentinel values
sleep 2
pgcopydb stream cleanup
