FROM pagila

WORKDIR /usr/src/pgcopydb
COPY ./copydb.sh copydb.sh
COPY ./dml.sql dml.sql
COPY ./ddl.sql ddl.sql
COPY ./extras.sql extras.sql

USER docker
CMD /usr/src/pgcopydb/copydb.sh
