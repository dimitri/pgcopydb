FROM pagila

# Writable work dir under /var/run/pgcopydb without the shared volume (see
# CDC-TCP-SENTINEL-WORKLOG): the pagila base doesn't chown it for the docker user.
USER root
RUN mkdir -p /var/run/pgcopydb && chown -R docker /var/run/pgcopydb

WORKDIR /usr/src/pgcopydb
COPY ./copydb.sh copydb.sh
COPY ./run-background-traffic.sh run-background-traffic.sh
COPY ./dml.sql dml.sql
COPY ./dml-bufsize.sql dml-bufsize.sql
COPY ./ddl.sql ddl.sql
COPY ./multi-wal-txn.sql multi-wal-txn.sql

USER docker
CMD ["/usr/src/pgcopydb/copydb.sh"]
