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

db="/var/lib/postgres/.local/share/pgcopydb/00000001-*.db"

sqlite3 ${db} <<EOF
select id, action, xid, lsn, substring(message, 1, 48) from output;

select hash, sql from stmt;

select id, action, xid, lsn, endlsn, stmt_hash, stmt_args from replay;
EOF

# cleanup
pgcopydb stream sentinel get

# make sure the inject service has had time to see the final sentinel values
sleep 2
pgcopydb stream cleanup
