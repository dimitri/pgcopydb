FROM pagila

WORKDIR /usr/src/pgcopydb
COPY ./copydb.sh copydb.sh
COPY ./dml.sql dml.sql
COPY ./ddl.sql ddl.sql
COPY ./special-dml.sql special-dml.sql
COPY ./special-ddl.sql special-ddl.sql
COPY ./000000010000000000000002.json 000000010000000000000002.json
COPY ./000000010000000000000002.sql 000000010000000000000002.sql

USER docker
CMD /usr/src/pgcopydb/copydb.sh
