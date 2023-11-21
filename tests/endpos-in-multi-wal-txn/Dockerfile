FROM pagila

WORKDIR /usr/src/pgcopydb
COPY ./copydb.sh copydb.sh
COPY ./ddl.sql ddl.sql
COPY ./multi-wal-txn.sql multi-wal-txn.sql

USER docker
CMD /usr/src/pgcopydb/copydb.sh
