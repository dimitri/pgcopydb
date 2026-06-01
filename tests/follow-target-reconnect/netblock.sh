#! /bin/bash
#
# Runs as a sidecar sharing the target container's network namespace.
# Waits for a trigger file written by inject.sh, then blocks port 5432
# with iptables REJECT for 8 seconds before restoring access.
#
set -e

TRIGGER=/var/run/pgcopydb/block_target
DONE=/var/run/pgcopydb/target_unblocked

until [ -f "$TRIGGER" ]
do
    sleep 0.5
done

rm -f "$TRIGGER"

iptables -I INPUT -p tcp --dport 5432 -j REJECT --reject-with tcp-reset

sleep 15

iptables -D INPUT -p tcp --dport 5432 -j REJECT --reject-with tcp-reset

touch "$DONE"
