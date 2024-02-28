FROM pagila

WORKDIR /usr/src/pgcopydb
COPY ./copydb.sh copydb.sh
COPY ./dml.sql dml.sql
COPY ./ddl.sql ddl.sql
COPY ./000000010000000000000002.json 000000010000000000000002.json
COPY ./000000010000000000000002.sql 000000010000000000000002.sql
COPY ./continued-txn.json continued-txn.json
COPY ./continued-txn.sql continued-txn.sql

USER docker
CMD /usr/src/pgcopydb/copydb.sh
