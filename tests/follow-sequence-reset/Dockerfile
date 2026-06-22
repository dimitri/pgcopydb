FROM pagila

WORKDIR /usr/src/pgcopydb
COPY ./copydb.sh copydb.sh
COPY ./dml.sql dml.sql

USER docker
CMD ["/usr/src/pgcopydb/copydb.sh"]
