#!/bin/sh

echo "host replication all all trust" >> ${PGDATA}/pg_hba.conf
pg_ctl reload
