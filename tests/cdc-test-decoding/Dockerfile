FROM pagila

WORKDIR /usr/src/pgcopydb
COPY ./copydb.sh copydb.sh
COPY ./dml.sql dml.sql
COPY ./ddl.sql ddl.sql
COPY ./continued-txn.json continued-txn.json
COPY ./continued-txn.sql continued-txn.sql

USER docker
CMD ["/usr/src/pgcopydb/copydb.sh"]
