FROM pagila

WORKDIR /usr/src/pgcopydb
COPY ./copydb.sh copydb.sh
COPY ./dml.sql dml.sql
COPY ./ddl.sql ddl.sql
COPY ./special-dml.sql special-dml.sql
COPY ./special-ddl.sql special-ddl.sql
COPY ./stmt.sql stmt.sql
COPY ./output.jsonl output.jsonl

USER docker
CMD ["/usr/src/pgcopydb/copydb.sh"]
