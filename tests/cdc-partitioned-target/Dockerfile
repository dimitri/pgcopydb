FROM pagila

WORKDIR /usr/src/pgcopydb
COPY ./copydb.sh copydb.sh
COPY ./source.sql source.sql
COPY ./target.sql target.sql
COPY ./dml.sql dml.sql
COPY ./dml-truncate.sql dml-truncate.sql

USER docker
CMD ["/usr/src/pgcopydb/copydb.sh"]
