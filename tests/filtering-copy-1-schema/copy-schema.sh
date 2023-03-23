#! /bin/bash

set -x
set -e

# This script expects the following environment variables to be set:
#
#  - PGCOPYDB_SOURCE_PGURI
#  - PGCOPYDB_TARGET_PGURI

#
# pgcopydb list extensions include a retry loop, so we use that as a proxy
# to depend on the source/target Postgres images to be ready
#
pgcopydb list extensions --source ${PGCOPYDB_SOURCE_PGURI}
pgcopydb list extensions --source ${PGCOPYDB_TARGET_PGURI}

# create 3 schemas with 2 tables each
psql -o /tmp/d.out -d ${PGCOPYDB_SOURCE_PGURI} -1 -f ./create-test-schemas.sql


#
# pgcopydb uses the environment variables
#
# enable file logging
export PGCOPYDB_LOG=./tmp/pgcopydb.log
export LOG_LEVEL=DEBUG

# copy one schema only
pgcopydb clone --${LOG_LEVEL} --source=${PGCOPYDB_SOURCE_PGURI} --target=${PGCOPYDB_TARGET_PGURI} --filter=./include-1-schema.ini --no-acl --no-owner
