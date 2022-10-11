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
# pgcopydb list tables include a retry loop, so we use that as a proxy to
# depend on the source/target Postgres images to be ready
#
pgcopydb list tables --source ${PGCOPYDB_SOURCE_PGURI}
pgcopydb list tables --source ${PGCOPYDB_TARGET_PGURI}

#
# Hack the pagila schema to make it compatible with Postgres 9.6. Remove:
#  - default_table_access_method
#  - PARTITION BY
#  - ALTER TABLE ... ATTACH PARTITION
#  - FOR EACH ROW EXECUTE [ FUNCTION => PROCEDURE ]
#
SCHEMA=/usr/src/pagila/pagila-schema.sql
grep -Ev 'default_table_access_method|ATTACH PARTITION' ${SCHEMA} > /tmp/schema.sql
perl -pi -e 's/PARTITION BY RANGE \(payment_date\)//' /tmp/schema.sql
perl -pi -e 's/FOR EACH ROW EXECUTE FUNCTION/FOR EACH ROW EXECUTE PROCEDURE/' /tmp/schema.sql

psql -o /tmp/s.out -d ${PGCOPYDB_SOURCE_PGURI} -1 -f /tmp/schema.sql
psql -o /tmp/d.out -d ${PGCOPYDB_SOURCE_PGURI} -1 -f /usr/src/pagila/pagila-data.sql

# alter the pagila schema to allow capturing DDLs without pkey
psql -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/ddl.sql

# pgcopydb copy db uses the environment variables
pgcopydb copy-db --follow

# cleanup
pgcopydb stream sentinel get

# make sure the inject service has had time to see the final sentinel values
sleep 2
pgcopydb stream cleanup
