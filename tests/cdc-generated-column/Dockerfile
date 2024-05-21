FROM pagila

WORKDIR /usr/src/pgcopydb
COPY ./copydb.sh copydb.sh
COPY ./ddl.sql ddl.sql
COPY ./dml.sql dml.sql

USER docker
CMD /usr/src/pgcopydb/copydb.sh
