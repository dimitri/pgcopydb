#! /bin/bash

set -x
set -e

# This script expects the following environment variables to be set:
#
#  - PGCOPYDB_SOURCE_PGURI
#  - PGCOPYDB_TARGET_PGURI
#  - PGCOPYDB_TABLE_JOBS
#  - PGCOPYDB_INDEX_JOBS

#
# Hack the pagila schema to make it compatible with Postgres 9.6. Remove:
#  - default_table_access_method
#  - PARTITION BY
#  - ALTER TABLE ... ATTACH PARTITION
#  - FOR EACH ROW EXECUTE [ FUNCTION => PROCEDURE ]
#
# To make it compatible with also Postgres 9.5, remove:
#  - idle_in_transaction_session_timeout
#
cp /usr/src/pagila/pagila-schema.sql /tmp/schema.sql
cp /usr/src/pagila/pagila-data.sql /tmp/data.sql

if [ "${PGVERSION}" == "9.5" ]
then
    sed -i -e '/idle_in_transaction_session_timeout/d' /tmp/data.sql
    sed -i -e '/idle_in_transaction_session_timeout/d' /tmp/schema.sql
fi

# Postgres 10 has support for declarative partitioning
if [ "${PGVERSION}" == "9.5" -o "${PGVERSION}" == "9.6" ]
then
    sed -i -e '/ATTACH PARTITION/d' /tmp/schema.sql
    perl -pi -e 's/PARTITION BY RANGE \(payment_date\)//' /tmp/schema.sql
fi

# Postgres 9.5, 9.6, and 10 all need that:
sed -i -e '/default_table_access_method/d' /tmp/schema.sql
perl -pi -e 's/FOR EACH ROW EXECUTE FUNCTION/FOR EACH ROW EXECUTE PROCEDURE/' /tmp/schema.sql

# make sure source and target databases are ready
pgcopydb ping

# install hacked pagila schema and data
psql -o /tmp/s.out -d ${PGCOPYDB_SOURCE_PGURI} -1 -f /tmp/schema.sql
psql -o /tmp/d.out -d ${PGCOPYDB_SOURCE_PGURI} -1 -f /tmp/data.sql

# alter the pagila schema to allow capturing DDLs without pkey
psql -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/ddl.sql

find ${TMPDIR}

# pgcopydb copy db uses the environment variables
pgcopydb clone --follow --notice --use-copy-binary

# cleanup
pgcopydb stream sentinel get

# make sure the inject service has had time to see the final sentinel values
sleep 2
pgcopydb stream cleanup

sql="select count(*), sum(amount) from payment"
psql -d ${PGCOPYDB_SOURCE_PGURI} -c "${sql}" > /tmp/s.out
psql -d ${PGCOPYDB_TARGET_PGURI} -c "${sql}" > /tmp/t.out

diff /tmp/s.out /tmp/t.out

# check the last value of sequence
sql="select last_value from payment_payment_id_seq"
psql -d ${PGCOPYDB_SOURCE_PGURI} -c "${sql}" > /tmp/s.out
psql -d ${PGCOPYDB_TARGET_PGURI} -c "${sql}" > /tmp/t.out

diff /tmp/s.out /tmp/t.out
