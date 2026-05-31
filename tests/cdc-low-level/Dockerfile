FROM pagila

WORKDIR /usr/src/pgcopydb
COPY ./copydb.sh copydb.sh
COPY ./dml.sql dml.sql
COPY ./ddl.sql ddl.sql
COPY ./pipeline-deadlock.sql pipeline-deadlock.sql

USER docker
CMD ["/usr/src/pgcopydb/copydb.sh"]
