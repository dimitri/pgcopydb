FROM pagila

WORKDIR /usr/src/pgcopydb
COPY ./copydb.sh copydb.sh
COPY ./include.ini include.ini
COPY ./exclude.ini exclude.ini

# unit tests
COPY ./sql ./test/sql
COPY ./expected ./test/expected

USER docker
WORKDIR /usr/src/pgcopydb/test/
CMD /usr/src/pgcopydb/copydb.sh
