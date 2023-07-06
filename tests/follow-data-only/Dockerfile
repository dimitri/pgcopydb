FROM pagila

WORKDIR /usr/src/pgcopydb
COPY ./copydb.sh copydb.sh
COPY ./run-background-traffic.sh run-background-traffic.sh
COPY ./dml.sql dml.sql
COPY ./dml-bufsize.sql dml-bufsize.sql
COPY ./ddl.sql ddl.sql
COPY ./multi-wal-txn.sql multi-wal-txn.sql

USER docker
CMD /usr/src/pgcopydb/copydb.sh
