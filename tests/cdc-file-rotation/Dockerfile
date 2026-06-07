FROM pagila

WORKDIR /usr/src/pgcopydb
COPY ./copydb.sh copydb.sh
COPY ./ddl.sql ddl.sql
COPY ./dml-small.sql dml-small.sql
COPY ./dml-large.sql dml-large.sql

USER docker
CMD ["/usr/src/pgcopydb/copydb.sh"]
