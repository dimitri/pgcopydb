FROM pagila

WORKDIR /usr/src/pgcopydb
COPY ./copydb.sh copydb.sh
COPY ./run-background-traffic.sh run-background-traffic.sh
COPY ./dml.sql dml.sql
COPY ./ddl.sql ddl.sql

USER docker
CMD /usr/src/pgcopydb/copydb.sh
