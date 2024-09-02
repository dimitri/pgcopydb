FROM pagila

WORKDIR /usr/src/pgcopydb
COPY ./copydb.sh copydb.sh
COPY ./dml.sql dml.sql
COPY ./dml2.sql dml2.sql
COPY ./ddl.sql ddl.sql

USER docker
CMD ["/usr/src/pgcopydb/copydb.sh"]
