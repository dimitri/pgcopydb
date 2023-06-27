#! /bin/bash

set -e

# Run transactions that insert a variable number of rows at a time. The number
# of rows inserted varies between 1 and 100. Each insert will occur in the
# background and finish quickly which avoids excessive connections.
sql="INSERT INTO table_a(f1) SELECT generate_series(1,(random() * 100 + 1)::int);"

while true; do
    psql -d ${PGCOPYDB_SOURCE_PGURI} -c "${sql}" &
    # To control the rate, we include a sleep of 0.1 seconds between each insert
    # operation.
    sleep 0.1
done
